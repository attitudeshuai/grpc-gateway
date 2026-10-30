package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	pb "github.com/grpc-ecosystem/grpc-gateway/v2/runtime/internal/examplepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const nonStandardUpdateMethod = pb.NonStandardService_Update_FullMethodName

type fixedBackoff time.Duration

func (d fixedBackoff) NextBackoff(int) time.Duration { return time.Duration(d) }

type statsRecorder struct {
	mu    sync.Mutex
	stats []runtime.RetryStats
}

func (r *statsRecorder) observe(s runtime.RetryStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats = append(r.stats, s)
}

func (r *statsRecorder) snapshot() []runtime.RetryStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]runtime.RetryStats, len(r.stats))
	copy(out, r.stats)
	return out
}

func (r *statsRecorder) last() runtime.RetryStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.stats) == 0 {
		return runtime.RetryStats{}
	}
	return r.stats[len(r.stats)-1]
}

// withoutDefaultTimeout isolates tests from other tests mutating the shared
// runtime.DefaultContextTimeout variable.
func withoutDefaultTimeout(t *testing.T) {
	t.Helper()
	previous := runtime.DefaultContextTimeout
	runtime.DefaultContextTimeout = 0
	t.Cleanup(func() { runtime.DefaultContextTimeout = previous })
}

func annotatedContext(t *testing.T, mux *runtime.ServeMux, req *http.Request, method string) context.Context {
	t.Helper()
	withoutDefaultTimeout(t)
	ctx, err := runtime.AnnotateContext(context.Background(), mux, req, method)
	if err != nil {
		t.Fatalf("AnnotateContext: %v", err)
	}
	return ctx
}

// flappyCall scripts a sequence of attempt results.
func flappyCall(t *testing.T, results []error) (runtime.UnaryCallFunc, *int32, *[]context.Context) {
	t.Helper()
	var calls int32
	ctxs := make([]context.Context, 0, len(results))
	return func(ctx context.Context, opts ...grpc.CallOption) (proto.Message, error) {
		n := atomic.AddInt32(&calls, 1)
		if int(n) > len(results) {
			t.Fatalf("upstream called %d times, scripted only %d", n, len(results))
		}
		ctxs = append(ctxs, ctx)
		if len(opts) != 2 {
			t.Errorf("attempt %d: got %d call options, want 2 (header + trailer)", n, len(opts))
		}
		if err := results[n-1]; err != nil {
			return nil, err
		}
		return nil, nil
	}, &calls, &ctxs
}

func TestInvokeUnary_NoPolicyCallsExactlyOnce(t *testing.T) {
	mux := runtime.NewServeMux()
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	withoutDefaultTimeout(t)
	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, err := runtime.AnnotateContext(parentCtx, mux, req, nonStandardUpdateMethod)
	if err != nil {
		t.Fatal(err)
	}

	var calls int32
	var seenCtx context.Context
	msg, md, err := runtime.InvokeUnary(ctx, nonStandardUpdateMethod, func(callCtx context.Context, opts ...grpc.CallOption) (proto.Message, error) {
		atomic.AddInt32(&calls, 1)
		seenCtx = callCtx
		if len(opts) != 2 {
			t.Errorf("got %d call options, want 2", len(opts))
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg != nil {
		t.Errorf("unexpected message: %v", msg)
	}
	if calls != 1 {
		t.Errorf("upstream calls = %d, want 1", calls)
	}
	// The legacy path must keep using the exact incoming context and still
	// return a metadata container.
	if seenCtx != ctx {
		t.Error("legacy path must not derive a new context")
	}
	_ = md
}

func TestInvokeUnary_FirstAttemptSucceeds(t *testing.T) {
	rec := &statsRecorder{}
	mux := runtime.NewServeMux(
		runtime.WithRetryPolicy(nonStandardUpdateMethod, runtime.RetryPolicy{MaxAttempts: 3, Backoff: fixedBackoff(time.Millisecond)}),
		runtime.WithRetryObserver(rec.observe),
	)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	ctx := annotatedContext(t, mux, req, nonStandardUpdateMethod)

	call, calls, _ := flappyCall(t, []error{nil})
	msg, _, err := runtime.InvokeUnary(ctx, nonStandardUpdateMethod, call)
	if err != nil || msg != nil {
		t.Fatalf("got (%v, %v), want (nil, nil)", msg, err)
	}
	if *calls != 1 {
		t.Errorf("calls = %d, want 1", *calls)
	}
	got := rec.last()
	if got.Attempts != 1 || got.Outcome != runtime.RetryOutcomeFirstAttemptSucceeded {
		t.Errorf("stats = %+v, want 1 attempt / first-attempt success", got)
	}
}

func TestInvokeUnary_RetriesThenSucceeds(t *testing.T) {
	rec := &statsRecorder{}
	mux := runtime.NewServeMux(
		runtime.WithRetryPolicy(nonStandardUpdateMethod, runtime.RetryPolicy{MaxAttempts: 4, Backoff: fixedBackoff(time.Millisecond)}),
		runtime.WithRetryObserver(rec.observe),
	)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	ctx := annotatedContext(t, mux, req, nonStandardUpdateMethod)

	wantErr := status.Error(codes.Unavailable, "try again")
	call, calls, ctxs := flappyCall(t, []error{wantErr, wantErr, nil})
	msg, _, err := runtime.InvokeUnary(ctx, nonStandardUpdateMethod, call)
	if err != nil || msg != nil {
		t.Fatalf("got (%v, %v), want success", msg, err)
	}
	if *calls != 3 {
		t.Errorf("calls = %d, want 3", *calls)
	}
	if len(*ctxs) != 3 {
		t.Fatalf("recorded %d attempt contexts, want 3", len(*ctxs))
	}
	if (*ctxs)[0] == (*ctxs)[1] || (*ctxs)[1] == (*ctxs)[2] {
		t.Error("attempts must use independent contexts")
	}
	got := rec.last()
	if got.Attempts != 3 || got.Outcome != runtime.RetryOutcomeRetriedSucceeded {
		t.Errorf("stats = %+v, want 3 attempts / success after retry", got)
	}
}

func TestInvokeUnary_NonRetryableFailuresAreNotRetried(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.PermissionDenied, codes.NotFound} {
		t.Run(code.String(), func(t *testing.T) {
			rec := &statsRecorder{}
			mux := runtime.NewServeMux(
				runtime.WithRetryPolicy(nonStandardUpdateMethod, runtime.RetryPolicy{MaxAttempts: 5, Backoff: fixedBackoff(time.Millisecond)}),
				runtime.WithRetryObserver(rec.observe),
			)
			req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
			ctx := annotatedContext(t, mux, req, nonStandardUpdateMethod)

			wantErr := status.Error(code, "nope")
			call, calls, _ := flappyCall(t, []error{wantErr})
			msg, _, err := runtime.InvokeUnary(ctx, nonStandardUpdateMethod, call)
			if msg != nil {
				t.Errorf("unexpected msg: %v", msg)
			}
			if status.Code(err) != code {
				t.Errorf("error code = %v, want %v; error must be passed through unchanged", status.Code(err), code)
			}
			if *calls != 1 {
				t.Errorf("calls = %d, want 1 for non-retryable code %v", *calls, code)
			}
			got := rec.last()
			if got.Attempts != 1 || got.Outcome != runtime.RetryOutcomeNonRetryableError {
				t.Errorf("stats = %+v, want 1 attempt / non-retryable", got)
			}
		})
	}
}

func TestInvokeUnary_BudgetExhaustedByMaxAttempts(t *testing.T) {
	rec := &statsRecorder{}
	mux := runtime.NewServeMux(
		runtime.WithRetryPolicy(nonStandardUpdateMethod, runtime.RetryPolicy{MaxAttempts: 3, Backoff: fixedBackoff(time.Millisecond)}),
		runtime.WithRetryObserver(rec.observe),
	)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	ctx := annotatedContext(t, mux, req, nonStandardUpdateMethod)

	wantErr := status.Error(codes.Unavailable, "down")
	call, calls, _ := flappyCall(t, []error{wantErr, wantErr, wantErr, wantErr})
	msg, _, err := runtime.InvokeUnary(ctx, nonStandardUpdateMethod, call)
	if msg != nil {
		t.Errorf("unexpected msg: %v", msg)
	}
	if status.Code(err) != codes.Unavailable {
		t.Errorf("error code = %v, want Unavailable (last failure decides the response)", status.Code(err))
	}
	if *calls != 3 {
		t.Errorf("calls = %d, want 3", *calls)
	}
	got := rec.last()
	if got.Attempts != 3 || got.Outcome != runtime.RetryOutcomeBudgetExhausted {
		t.Errorf("stats = %+v, want 3 attempts / budget exhausted", got)
	}
}

func TestInvokeUnary_ClientDeadlineLeavesNoRoomForAnotherAttempt(t *testing.T) {
	rec := &statsRecorder{}
	mux := runtime.NewServeMux(
		runtime.WithRetryPolicy(nonStandardUpdateMethod, runtime.RetryPolicy{MaxAttempts: 5, Backoff: fixedBackoff(time.Second)}),
		runtime.WithRetryObserver(rec.observe),
	)
	// The Grpc-Timeout header is the client deadline.
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	req.Header.Set("Grpc-Timeout", "30m")
	ctx := annotatedContext(t, mux, req, nonStandardUpdateMethod)

	wantErr := status.Error(codes.Unavailable, "down")
	call, calls, _ := flappyCall(t, []error{wantErr, wantErr})
	start := time.Now()
	_, _, err := runtime.InvokeUnary(ctx, nonStandardUpdateMethod, call)
	if status.Code(err) != codes.Unavailable {
		t.Errorf("error code = %v, want Unavailable", status.Code(err))
	}
	if *calls != 1 {
		t.Errorf("calls = %d, want 1: remaining budget must not fit the backoff", *calls)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("InvokeUnary took %v, must not wait for the full backoff on a tight budget", elapsed)
	}
	got := rec.last()
	if got.Attempts != 1 || got.Outcome != runtime.RetryOutcomeBudgetExhausted {
		t.Errorf("stats = %+v, want 1 attempt / budget exhausted", got)
	}
}

func TestInvokeUnary_LocalTotalTimeoutCapsSharedBudget(t *testing.T) {
	rec := &statsRecorder{}
	mux := runtime.NewServeMux(
		runtime.WithRetryPolicy(nonStandardUpdateMethod, runtime.RetryPolicy{
			MaxAttempts:  10,
			TotalTimeout: 80 * time.Millisecond,
			Backoff:      fixedBackoff(30 * time.Millisecond),
		}),
		runtime.WithRetryObserver(rec.observe),
	)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	ctx := annotatedContext(t, mux, req, nonStandardUpdateMethod)

	wantErr := status.Error(codes.Unavailable, "down")
	// Script more results than attempts should ever be made.
	results := make([]error, 10)
	for i := range results {
		results[i] = wantErr
	}
	call, calls, _ := flappyCall(t, results)
	_, _, err := runtime.InvokeUnary(ctx, nonStandardUpdateMethod, call)
	if status.Code(err) != codes.Unavailable {
		t.Errorf("error code = %v, want Unavailable", status.Code(err))
	}
	// Attempts start at ~0ms and ~30ms; before a fourth attempt at ~60ms the
	// remaining ~20ms cannot cover the 30ms backoff, so exactly three attempts
	// are started under the shared budget.
	if *calls != 3 {
		t.Errorf("calls = %d, want 3 under the shared total budget", *calls)
	}
	got := rec.last()
	if got.Attempts != 3 || got.Outcome != runtime.RetryOutcomeBudgetExhausted {
		t.Errorf("stats = %+v, want 3 attempts / budget exhausted", got)
	}
}

func TestInvokeUnary_ClientDisconnectStopsRetry(t *testing.T) {
	t.Run("during backoff", func(t *testing.T) {
		rec := &statsRecorder{}
		mux := runtime.NewServeMux(
			runtime.WithRetryPolicy(nonStandardUpdateMethod, runtime.RetryPolicy{MaxAttempts: 5, Backoff: fixedBackoff(10 * time.Second)}),
			runtime.WithRetryObserver(rec.observe),
		)
		reqCtx, cancelReq := context.WithCancel(context.Background())
		req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil).WithContext(reqCtx)
		ctx := annotatedContext(t, mux, req, nonStandardUpdateMethod)

		wantErr := status.Error(codes.Unavailable, "down")
		call, calls, _ := flappyCall(t, []error{wantErr, wantErr})
		done := make(chan struct{})
		go func() {
			_, _, _ = runtime.InvokeUnary(ctx, nonStandardUpdateMethod, call)
			close(done)
		}()

		// Wait for the first attempt, then simulate the client going away.
		deadline := time.Now().Add(2 * time.Second)
		for atomic.LoadInt32(calls) == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		cancelReq()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("InvokeUnary did not return after the client disconnected; backoff timer leaked")
		}
		if *calls != 1 {
			t.Errorf("calls = %d, want 1 after client disconnect", *calls)
		}
		got := rec.last()
		if got.Attempts != 1 || got.Outcome != runtime.RetryOutcomeAborted {
			t.Errorf("stats = %+v, want 1 attempt / aborted", got)
		}
		// No late attempt must arrive after the abort.
		time.Sleep(50 * time.Millisecond)
		if *calls != 1 {
			t.Errorf("a retry was started after the client disconnected: calls = %d", *calls)
		}
	})

	t.Run("already gone before retry decision", func(t *testing.T) {
		rec := &statsRecorder{}
		mux := runtime.NewServeMux(
			runtime.WithRetryPolicy(nonStandardUpdateMethod, runtime.RetryPolicy{MaxAttempts: 5, Backoff: fixedBackoff(time.Second)}),
			runtime.WithRetryObserver(rec.observe),
		)
		reqCtx, cancelReq := context.WithCancel(context.Background())
		cancelReq()
		req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil).WithContext(reqCtx)
		ctx := annotatedContext(t, mux, req, nonStandardUpdateMethod)

		wantErr := status.Error(codes.Unavailable, "down")
		call, calls, _ := flappyCall(t, []error{wantErr, wantErr})
		_, _, err := runtime.InvokeUnary(ctx, nonStandardUpdateMethod, call)
		if status.Code(err) != codes.Unavailable {
			t.Errorf("error code = %v, want Unavailable", status.Code(err))
		}
		if *calls != 1 {
			t.Errorf("calls = %d, want 1", *calls)
		}
		got := rec.last()
		if got.Outcome != runtime.RetryOutcomeAborted {
			t.Errorf("stats = %+v, want aborted", got)
		}
	})
}

func TestInvokeUnary_ServiceAndMethodDeclarations(t *testing.T) {
	const (
		serviceTarget = "/grpc.gateway.runtime.internal.examplepb.NonStandardService"
		otherMethod   = serviceTarget + "/UpdateWithJSONNames"
	)
	mux := runtime.NewServeMux(
		runtime.WithRetryPolicies(map[string]runtime.RetryPolicy{
			serviceTarget:           {MaxAttempts: 2, Backoff: fixedBackoff(time.Millisecond)},
			nonStandardUpdateMethod: {MaxAttempts: 4, Backoff: fixedBackoff(time.Millisecond)},
		}),
	)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)

	wantErr := status.Error(codes.Unavailable, "down")
	ctx := annotatedContext(t, mux, req, nonStandardUpdateMethod)
	call1, calls1, _ := flappyCall(t, []error{wantErr, wantErr, wantErr, wantErr})
	_, _, _ = runtime.InvokeUnary(ctx, nonStandardUpdateMethod, call1)
	if *calls1 != 4 {
		t.Errorf("method-specific attempts = %d, want 4", *calls1)
	}

	// A different method on the same service uses the service-wide policy.
	ctx2 := annotatedContext(t, mux, httptest.NewRequest(http.MethodGet, "http://example.com/", nil), otherMethod)
	call2, calls2, _ := flappyCall(t, []error{wantErr, wantErr, wantErr})
	_, _, _ = runtime.InvokeUnary(ctx2, otherMethod, call2)
	if *calls2 != 2 {
		t.Errorf("service-wide attempts = %d, want 2", *calls2)
	}

	// An undeclared service is never retried.
	const foreign = "/some.other.Service/Do"
	ctx3 := annotatedContext(t, mux, httptest.NewRequest(http.MethodGet, "http://example.com/", nil), foreign)
	call3, calls3, _ := flappyCall(t, []error{wantErr})
	_, _, err := runtime.InvokeUnary(ctx3, foreign, call3)
	if status.Code(err) != codes.Unavailable || *calls3 != 1 {
		t.Errorf("undeclared target: calls=%d err=%v, want 1 call and passthrough error", *calls3, err)
	}
}

func TestInvokeUnary_CustomRetryableCodes(t *testing.T) {
	mux := runtime.NewServeMux(
		runtime.WithRetryPolicy(nonStandardUpdateMethod, runtime.RetryPolicy{
			MaxAttempts: 2,
			Codes:       []codes.Code{codes.DeadlineExceeded},
			Backoff:     fixedBackoff(time.Millisecond),
		}),
	)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	ctx := annotatedContext(t, mux, req, nonStandardUpdateMethod)

	// Unavailable is not in the declared set: immediate failure.
	call, calls, _ := flappyCall(t, []error{status.Error(codes.Unavailable, "x"), status.Error(codes.Unavailable, "x")})
	_, _, _ = runtime.InvokeUnary(ctx, nonStandardUpdateMethod, call)
	if *calls != 1 {
		t.Errorf("attempts = %d, want 1 for code outside the declared set", *calls)
	}

	// DeadlineExceeded is declared retryable.
	ctx2 := annotatedContext(t, mux, httptest.NewRequest(http.MethodGet, "http://example.com/", nil), nonStandardUpdateMethod)
	call2, calls2, _ := flappyCall(t, []error{status.Error(codes.DeadlineExceeded, "x"), nil})
	msg, _, err := runtime.InvokeUnary(ctx2, nonStandardUpdateMethod, call2)
	if err != nil || msg != nil {
		t.Fatalf("retry on declared code: got (%v, %v), want success", msg, err)
	}
	if *calls2 != 2 {
		t.Errorf("attempts = %d, want 2", *calls2)
	}
}

// --- End-to-end behavior against a real gRPC server, exercising the exact
// glue emitted by the generated registration functions. ---

type flakyNonStandardServer struct {
	pb.UnimplementedNonStandardServiceServer
	calls    atomic.Int32
	failures atomic.Int32
}

func (s *flakyNonStandardServer) Update(ctx context.Context, req *pb.NonStandardUpdateRequest) (*pb.NonStandardMessage, error) {
	n := s.calls.Add(1)
	// Every attempt carries distinct header and trailer values so that
	// metadata from failed attempts leaking into the final response would be
	// observable.
	_ = grpc.SetHeader(ctx, metadata.Pairs("x-attempt", strconv.FormatInt(int64(n), 10)))
	_ = grpc.SetTrailer(ctx, metadata.Pairs("x-attempt-trailer", strconv.FormatInt(int64(n), 10)))
	if n <= s.failures.Load() {
		return nil, status.Error(codes.Unavailable, "boom")
	}
	if req.GetBody() != nil {
		return req.GetBody(), nil
	}
	return &pb.NonStandardMessage{}, nil
}

func startFlakyGateway(t *testing.T, failures int32, muxOpts ...runtime.ServeMuxOption) (*httptest.Server, *flakyNonStandardServer, *statsRecorder) {
	t.Helper()
	withoutDefaultTimeout(t)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	flaky := &flakyNonStandardServer{}
	flaky.failures.Store(failures)
	pb.RegisterNonStandardServiceServer(server, flaky)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewNonStandardServiceClient(conn)

	rec := &statsRecorder{}
	opts := append([]runtime.ServeMuxOption{}, muxOpts...)
	opts = append(opts, runtime.WithRetryObserver(rec.observe))
	mux := runtime.NewServeMux(opts...)
	marshaler := &runtime.JSONPb{}
	_ = mux.HandlePath(http.MethodPost, "/v1/update", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		annotatedContext, err := runtime.AnnotateContext(ctx, mux, r, nonStandardUpdateMethod)
		if err != nil {
			runtime.HTTPError(ctx, mux, marshaler, w, r, err)
			return
		}
		msg, md, err := runtime.InvokeUnary(annotatedContext, nonStandardUpdateMethod, func(callCtx context.Context, callOpts ...grpc.CallOption) (proto.Message, error) {
			return client.Update(callCtx, &pb.NonStandardUpdateRequest{Body: &pb.NonStandardMessage{}}, callOpts...)
		})
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, marshaler, w, r, err)
			return
		}
		runtime.ForwardResponseMessage(annotatedContext, mux, marshaler, w, r, msg)
	})

	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)
	return httpServer, flaky, rec
}

func retryPolicy(maxAttempts int, backoff time.Duration, totalTimeout time.Duration) runtime.ServeMuxOption {
	return runtime.WithRetryPolicy(nonStandardUpdateMethod, runtime.RetryPolicy{
		MaxAttempts:  maxAttempts,
		TotalTimeout: totalTimeout,
		Backoff:      fixedBackoff(backoff),
	})
}

func doUpdate(t *testing.T, url string, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/v1/update", nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func TestE2E_FailuresThenSuccessWritesSingleResponseWithFinalMetadata(t *testing.T) {
	gw, flaky, rec := startFlakyGateway(t, 2, retryPolicy(4, time.Millisecond, 0))

	resp, body := doUpdate(t, gw.URL, map[string]string{"TE": "trailers"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", resp.StatusCode, body)
	}
	if flaky.calls.Load() != 3 {
		t.Errorf("upstream attempts = %d, want 3", flaky.calls.Load())
	}
	// The success body must be written exactly once.
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("response body must be a single JSON object, got %q: %v", body, err)
	}
	// Headers and trailers must come from the last attempt only.
	if got := resp.Header.Get("Grpc-Metadata-X-Attempt"); got != "3" {
		t.Errorf("response header x-attempt = %q, want 3 (failed attempts must not leak)", got)
	}
	if got := resp.Trailer.Get("Grpc-Trailer-X-Attempt-Trailer"); got != "3" {
		t.Errorf("response trailer x-attempt-trailer = %q, want 3 (failed attempts must not leak)", got)
	}
	stats := rec.last()
	if stats.Attempts != 3 || stats.Outcome != runtime.RetryOutcomeRetriedSucceeded {
		t.Errorf("stats = %+v, want 3 attempts / retried success", stats)
	}
}

func TestE2E_AllAttemptsFailWritesSingleMappedError(t *testing.T) {
	gw, flaky, rec := startFlakyGateway(t, 5, retryPolicy(3, time.Millisecond, 0))

	resp, body := doUpdate(t, gw.URL, map[string]string{"TE": "trailers"})
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body: %s", resp.StatusCode, body)
	}
	if flaky.calls.Load() != 3 {
		t.Errorf("upstream attempts = %d, want 3", flaky.calls.Load())
	}
	var decoded map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(body), &decoded); err != nil {
		t.Fatalf("error body must be a single JSON object, got %q: %v", body, err)
	}
	if code, _ := decoded["code"].(float64); int(code) != int(codes.Unavailable) {
		t.Errorf("error body code = %v, want %d; body: %s", decoded["code"], codes.Unavailable, body)
	}
	// Metadata of the last failing attempt only.
	if got := resp.Header.Get("Grpc-Metadata-X-Attempt"); got != "3" {
		t.Errorf("error response header x-attempt = %q, want 3", got)
	}
	if got := resp.Trailer.Get("Grpc-Trailer-X-Attempt-Trailer"); got != "3" {
		t.Errorf("error response trailer = %q, want 3", got)
	}
	stats := rec.last()
	if stats.Attempts != 3 || stats.Outcome != runtime.RetryOutcomeBudgetExhausted {
		t.Errorf("stats = %+v, want 3 attempts / budget exhausted", stats)
	}
}

func TestE2E_TightClientDeadlineDoesNotStartSecondAttempt(t *testing.T) {
	// Backoff far exceeds the client deadline: after the first immediate
	// failure the remaining budget cannot cover another attempt.
	gw, flaky, rec := startFlakyGateway(t, 5, retryPolicy(5, time.Second, 0))

	resp, body := doUpdate(t, gw.URL, map[string]string{"Grpc-Timeout": "30m"})
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body: %s", resp.StatusCode, body)
	}
	if flaky.calls.Load() != 1 {
		t.Errorf("upstream attempts = %d, want 1 under tight deadline", flaky.calls.Load())
	}
	stats := rec.last()
	if stats.Attempts != 1 || stats.Outcome != runtime.RetryOutcomeBudgetExhausted {
		t.Errorf("stats = %+v, want 1 attempt / budget exhausted", stats)
	}
}

func TestE2E_ClientDisconnectStartsNoFurtherAttempt(t *testing.T) {
	gw, flaky, rec := startFlakyGateway(t, 5, retryPolicy(5, 10*time.Second, 0))

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gw.URL+"/v1/update", nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = http.DefaultClient.Do(req)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for flaky.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()

	// Give the gateway plenty of time to misbehave.
	time.Sleep(200 * time.Millisecond)
	if got := flaky.calls.Load(); got != 1 {
		t.Errorf("upstream attempts after disconnect = %d, want 1", got)
	}
	stats := rec.last()
	if stats.Attempts != 1 || stats.Outcome != runtime.RetryOutcomeAborted {
		t.Errorf("stats = %+v, want 1 attempt / aborted", stats)
	}
}

func TestE2E_NoPolicyBehavesAsSingleCall(t *testing.T) {
	gw, flaky, rec := startFlakyGateway(t, 1)

	resp, body := doUpdate(t, gw.URL, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body: %s", resp.StatusCode, body)
	}
	if flaky.calls.Load() != 1 {
		t.Errorf("upstream attempts = %d, want 1 without a declared policy", flaky.calls.Load())
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("error body: %v", err)
	}
	if len(rec.snapshot()) != 0 {
		t.Errorf("observer must not fire without a policy, got %+v", rec.snapshot())
	}

	// And a healthy upstream succeeds on the first call.
	gw2, flaky2, _ := startFlakyGateway(t, 0)
	resp2, body2 := doUpdate(t, gw2.URL, map[string]string{"TE": "trailers"})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", resp2.StatusCode, body2)
	}
	if flaky2.calls.Load() != 1 {
		t.Errorf("upstream attempts = %d, want 1", flaky2.calls.Load())
	}
	if got := resp2.Header.Get("Grpc-Metadata-X-Attempt"); got != "1" {
		t.Errorf("header = %q, want 1", got)
	}
}
