package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// delayHealthServer answers Health.Check with SERVING after a configurable
// delay, so in-flight request draining can be exercised deterministically.
type delayHealthServer struct {
	healthpb.UnimplementedHealthServer
	delayNanos atomic.Int64
	started    chan struct{}
	startOnce  sync.Once
}

func (s *delayHealthServer) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if s.started != nil {
		s.startOnce.Do(func() { close(s.started) })
	}
	delay := time.Duration(s.delayNanos.Load())
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

// countingListener tracks server-side TCP connections, which is how the test
// observes the real number of connections established and closed.
type countingListener struct {
	net.Listener
	active atomic.Int64
	closed atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.active.Add(1)
	return &countingConn{Conn: c, listener: l}, nil
}

type countingConn struct {
	net.Conn
	listener *countingListener
	once     sync.Once
}

func (c *countingConn) Close() error {
	c.once.Do(func() {
		c.listener.active.Add(-1)
		c.listener.closed.Add(1)
	})
	return c.Conn.Close()
}

type upstreamFixture struct {
	t        *testing.T
	addr     string
	server   *grpc.Server
	listener *countingListener
	health   *delayHealthServer
}

func newUpstreamFixture(t *testing.T) *upstreamFixture {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cl := &countingListener{Listener: lis}
	srv := grpc.NewServer()
	h := &delayHealthServer{}
	healthpb.RegisterHealthServer(srv, h)
	go func() { _ = srv.Serve(cl) }()
	t.Cleanup(srv.Stop)
	return &upstreamFixture{
		t:        t,
		addr:     lis.Addr().String(),
		server:   srv,
		listener: cl,
		health:   h,
	}
}

// registerHealth mimics a generated Register<Service>HandlerFromEndpoint call:
// it dials through the mux and mounts one HTTP route that calls Health.Check.
func registerHealth(ctx context.Context, mux *runtime.ServeMux, endpoint, path string, opts ...grpc.DialOption) error {
	return mux.RegisterFromEndpoint(ctx, endpoint, opts, func(ctx context.Context, conn *grpc.ClientConn) (err error) {
		client := healthpb.NewHealthClient(conn)
		ctx, finish, err := mux.BeginRegistration(ctx)
		if err != nil {
			return err
		}
		defer finish(&err)
		return mux.HandlePath(http.MethodGet, path, func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
			resp, err := client.Check(r.Context(), &healthpb.HealthCheckRequest{})
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"status": resp.GetStatus().String()})
		})
	})
}

func mustDoRequest(t *testing.T, mux *runtime.ServeMux, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

func sharedDialOptions(t *testing.T) ([]grpc.DialOption, *atomic.Int64) {
	t.Helper()
	var intercepted atomic.Int64
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		intercepted.Add(1)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptor),
	}, &intercepted
}

// Two services on the same endpoint with the same dial options share one
// connection: only one dial happens, both services work, the configured
// interceptor runs for both, and the connection closes after the last user
// releases it.
func TestUpstreamConnsSharedPerEndpoint(t *testing.T) {
	fix := newUpstreamFixture(t)
	pool := runtime.NewUpstreamConns()
	mux := runtime.NewServeMux(runtime.WithUpstreamConns(pool))
	opts, intercepted := sharedDialOptions(t)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	if err := registerHealth(ctx1, mux, fix.addr, "/a", opts...); err != nil {
		t.Fatalf("register service A: %v", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	if err := registerHealth(ctx2, mux, fix.addr, "/b", opts...); err != nil {
		t.Fatalf("register service B: %v", err)
	}

	if stats := pool.Stats(); stats.Dials != 1 {
		t.Fatalf("Dials = %d, want 1 (one shared connection)", stats.Dials)
	} else if stats.ActiveConnections != 1 {
		t.Fatalf("ActiveConnections = %d, want 1", stats.ActiveConnections)
	} else if stats.References != 2 {
		t.Fatalf("References = %d, want 2", stats.References)
	}

	if rec := mustDoRequest(t, mux, "/a"); rec.Code != http.StatusOK {
		t.Fatalf("GET /a = %d, want 200", rec.Code)
	}
	if rec := mustDoRequest(t, mux, "/b"); rec.Code != http.StatusOK {
		t.Fatalf("GET /b = %d, want 200", rec.Code)
	}
	if got := intercepted.Load(); got != 2 {
		t.Fatalf("unary interceptor invoked %d times, want 2 (old interceptor behavior preserved)", got)
	}
	waitFor(t, time.Second, func() bool { return fix.listener.active.Load() == 1 }, "one server-side TCP connection")

	// Releasing the first user leaves the connection alive for the second.
	cancel1()
	waitFor(t, time.Second, func() bool { return pool.Stats().References == 1 }, "reference count to drop to 1")
	if stats := pool.Stats(); stats.ActiveConnections != 1 || stats.Closes != 0 {
		t.Fatalf("after first release: ActiveConnections=%d Closes=%d, want 1 and 0", stats.ActiveConnections, stats.Closes)
	}
	if rec := mustDoRequest(t, mux, "/b"); rec.Code != http.StatusOK {
		t.Fatalf("GET /b after releasing A = %d, want 200 (surviving service still usable)", rec.Code)
	}

	// The last release closes the single shared connection.
	cancel2()
	waitFor(t, time.Second, func() bool {
		s := pool.Stats()
		return s.ActiveConnections == 0 && s.Closes == 1
	}, "shared connection to close on last release")
	waitFor(t, time.Second, func() bool { return fix.listener.closed.Load() == 1 }, "one server-side connection close")
}

// A failed registration rolls back the whole group: its routes disappear and
// no extra connection lingers, while the already-registered service is
// unaffected.
func TestUpstreamConnsRegistrationFailureRollback(t *testing.T) {
	fix := newUpstreamFixture(t)
	pool := runtime.NewUpstreamConns()
	mux := runtime.NewServeMux(runtime.WithUpstreamConns(pool))
	opts, _ := sharedDialOptions(t)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	if err := registerHealth(ctx1, mux, fix.addr, "/a", opts...); err != nil {
		t.Fatalf("register service A: %v", err)
	}

	boom := errors.New("simulated mid-registration failure")
	err := mux.RegisterFromEndpoint(ctx1, fix.addr, opts, func(ctx context.Context, conn *grpc.ClientConn) error {
		ctx, finish, beginErr := mux.BeginRegistration(ctx)
		if beginErr != nil {
			return beginErr
		}
		var regErr error
		defer finish(&regErr)
		if regErr = mux.HandlePath(http.MethodGet, "/half", func(http.ResponseWriter, *http.Request, map[string]string) {}); regErr != nil {
			return regErr
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("registration error = %v, want %v (ordinary registration failure)", err, boom)
	}

	if rec := mustDoRequest(t, mux, "/half"); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /half = %d, want 404 (rolled-back route must not linger)", rec.Code)
	}
	if rec := mustDoRequest(t, mux, "/a"); rec.Code != http.StatusOK {
		t.Fatalf("GET /a = %d, want 200 (first service still works)", rec.Code)
	}
	if stats := pool.Stats(); stats.Dials != 1 || stats.ActiveConnections != 1 || stats.References != 1 {
		t.Fatalf("stats after failed second registration = %+v, want one shared conn with one reference", stats)
	}
}

// After Shutdown, registrations fail with a distinct, attributable error;
// shutting down again is idempotent.
func TestUpstreamConnsRegisterAfterShutdown(t *testing.T) {
	fix := newUpstreamFixture(t)
	pool := runtime.NewUpstreamConns()
	mux := runtime.NewServeMux(runtime.WithUpstreamConns(pool))

	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown should be idempotent, got %v", err)
	}

	ctx := context.Background()
	if err := registerHealth(ctx, mux, fix.addr, "/a", grpc.WithTransportCredentials(insecure.NewCredentials())); !errors.Is(err, runtime.ErrUpstreamClosed) {
		t.Fatalf("register after close = %v, want errors.Is(ErrUpstreamClosed)", err)
	}
	// Route-only registrations are rejected too.
	if _, _, err := mux.BeginRegistration(ctx); !errors.Is(err, runtime.ErrUpstreamClosed) {
		t.Fatalf("BeginRegistration after close = %v, want ErrUpstreamClosed", err)
	}
}

// In-flight requests are allowed to finish within the declared drain window:
// Shutdown waits, returns nil, and the response completes normally.
func TestUpstreamConnsDrainInFlight(t *testing.T) {
	fix := newUpstreamFixture(t)
	fix.health.delayNanos.Store(int64(300 * time.Millisecond))
	fix.health.started = make(chan struct{})

	pool := runtime.NewUpstreamConns()
	mux := runtime.NewServeMux(runtime.WithUpstreamConns(pool))
	opts, _ := sharedDialOptions(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := registerHealth(ctx, mux, fix.addr, "/slow", opts...); err != nil {
		t.Fatalf("register: %v", err)
	}
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	type result struct {
		code int
		body []byte
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		resp, err := http.Get(httpServer.URL + "/slow")
		if err != nil {
			resultCh <- result{err: err}
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		resultCh <- result{code: resp.StatusCode, body: body}
	}()
	<-fix.health.started

	shutdownDone := make(chan error, 1)
	go func() {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer drainCancel()
		shutdownDone <- pool.Shutdown(drainCtx)
	}()

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown while request drains: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return after in-flight request finished")
	}

	res := <-resultCh
	if res.err != nil {
		t.Fatalf("in-flight request failed: %v, want it to complete while draining", res.err)
	}
	if res.code != http.StatusOK {
		t.Fatalf("in-flight response = %d %s, want 200", res.code, res.body)
	}
}

// Requests still running when the drain window elapses are ended
// deterministically: connections close and Shutdown reports the drain
// timeout distinctly; no connection stays open behind the shutdown.
func TestUpstreamConnsDrainTimeout(t *testing.T) {
	fix := newUpstreamFixture(t)
	fix.health.delayNanos.Store(int64(30 * time.Second))
	fix.health.started = make(chan struct{})

	pool := runtime.NewUpstreamConns()
	mux := runtime.NewServeMux(runtime.WithUpstreamConns(pool))
	opts, _ := sharedDialOptions(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := registerHealth(ctx, mux, fix.addr, "/stuck", opts...); err != nil {
		t.Fatalf("register: %v", err)
	}
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	ended := make(chan struct{})
	go func() {
		resp, _ := http.Get(httpServer.URL + "/stuck")
		if resp != nil {
			_ = resp.Body.Close()
		}
		close(ended)
	}()
	<-fix.health.started

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer drainCancel()
	err := pool.Shutdown(drainCtx)
	if !errors.Is(err, runtime.ErrUpstreamDrainTimeout) {
		t.Fatalf("Shutdown = %v, want errors.Is(ErrUpstreamDrainTimeout)", err)
	}
	if stats := pool.Stats(); stats.ActiveConnections != 0 {
		t.Fatalf("ActiveConnections after forced shutdown = %d, want 0 (deterministic cleanup)", stats.ActiveConnections)
	}
	// The stuck request must be ended, not left running against a closed conn.
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request kept running after forced shutdown")
	}
}

// Without WithUpstreamConns every registration dials and closes its own
// private connection, exactly as before.
func TestUpstreamConnsLegacyPrivateConnections(t *testing.T) {
	fix := newUpstreamFixture(t)
	mux := runtime.NewServeMux()
	dials := new(atomic.Int64)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}),
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	if err := registerHealth(ctx1, mux, fix.addr, "/a", opts...); err != nil {
		t.Fatalf("register A: %v", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	if err := registerHealth(ctx2, mux, fix.addr, "/b", opts...); err != nil {
		t.Fatalf("register B: %v", err)
	}

	if rec := mustDoRequest(t, mux, "/a"); rec.Code != http.StatusOK {
		t.Fatalf("GET /a = %d, want 200", rec.Code)
	}
	if rec := mustDoRequest(t, mux, "/b"); rec.Code != http.StatusOK {
		t.Fatalf("GET /b = %d, want 200", rec.Code)
	}
	waitFor(t, time.Second, func() bool { return dials.Load() == 2 }, "two private dial attempts")
	waitFor(t, time.Second, func() bool { return fix.listener.active.Load() == 2 }, "two server-side connections")

	cancel1()
	cancel2()
	waitFor(t, time.Second, func() bool { return fix.listener.active.Load() == 0 }, "both private connections to close")
	waitFor(t, time.Second, func() bool { return fix.listener.closed.Load() == 2 }, "two server-side connection closes")
}

// A failed legacy registration still rolls back its routes and closes the
// private connection it dialed.
func TestUpstreamConnsLegacyFailureRollback(t *testing.T) {
	fix := newUpstreamFixture(t)
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	boom := errors.New("boom")
	err := mux.RegisterFromEndpoint(ctx, fix.addr, opts, func(ctx context.Context, conn *grpc.ClientConn) (regErr error) {
		ctx, finish, beginErr := mux.BeginRegistration(ctx)
		if beginErr != nil {
			return beginErr
		}
		defer finish(&regErr)
		if err := mux.HandlePath(http.MethodGet, "/never", func(http.ResponseWriter, *http.Request, map[string]string) {}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("legacy registration error = %v, want %v", err, boom)
	}
	if rec := mustDoRequest(t, mux, "/never"); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /never = %d, want 404", rec.Code)
	}
}

// Different groups of dial options get different connections even for the same
// endpoint.
func TestUpstreamConnsDistinctDialOptions(t *testing.T) {
	fix := newUpstreamFixture(t)
	pool := runtime.NewUpstreamConns()
	mux := runtime.NewServeMux(runtime.WithUpstreamConns(pool))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	optsA := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	optsB := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUserAgent("different-options"),
	}
	if err := registerHealth(ctx, mux, fix.addr, "/a", optsA...); err != nil {
		t.Fatalf("register A: %v", err)
	}
	if err := registerHealth(ctx, mux, fix.addr, "/b", optsB...); err != nil {
		t.Fatalf("register B: %v", err)
	}
	if stats := pool.Stats(); stats.Dials != 2 || stats.ActiveConnections != 2 {
		t.Fatalf("stats = %+v, want two distinct connections for distinct options", stats)
	}
}

// BeginRegistration provides transactional route registration even without
// any upstream connection involved.
func TestBeginRegistrationRollback(t *testing.T) {
	mux := runtime.NewServeMux()
	ctx := context.Background()

	ctx, finish, err := mux.BeginRegistration(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := mux.HandlePath(http.MethodGet, "/rolled-back", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		fmt.Fprint(w, "should not survive")
	}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	regErr := errors.New("registration failed")
	finish(&regErr)
	if rec := mustDoRequest(t, mux, "/rolled-back"); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /rolled-back = %d, want 404 after rollback", rec.Code)
	}

	_, finish2, err := mux.BeginRegistration(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := mux.HandlePath(http.MethodGet, "/committed", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		fmt.Fprint(w, "ok")
	}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	var successErr error
	finish2(&successErr)
	if rec := mustDoRequest(t, mux, "/committed"); rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("GET /committed = %d %q, want 200 ok", rec.Code, rec.Body.String())
	}
}

// Registrations and releases interleaved with each other and with live
// requests stay consistent; run under -race to validate the concurrency
// guarantees.
func TestUpstreamConnsConcurrentRegisterDeregister(t *testing.T) {
	fix := newUpstreamFixture(t)
	pool := runtime.NewUpstreamConns()
	mux := runtime.NewServeMux(runtime.WithUpstreamConns(pool))
	opts, _ := sharedDialOptions(t)

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			path := fmt.Sprintf("/svc/%d", i)
			if err := registerHealth(ctx, mux, fix.addr, path, opts...); err != nil {
				t.Errorf("register %d: %v", i, err)
				return
			}
			if rec := mustDoRequest(t, mux, path); rec.Code != http.StatusOK {
				t.Errorf("GET %s = %d, want 200", path, rec.Code)
			}
			cancel()
		}(i)
	}
	wg.Wait()

	waitFor(t, 5*time.Second, func() bool {
		s := pool.Stats()
		return s.ActiveConnections == 0 && s.References == 0
	}, "all shared connections released")
	if dials := pool.Stats().Dials; dials < 1 {
		t.Fatalf("Dials = %d, want at least 1 shared connection", dials)
	}
	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
