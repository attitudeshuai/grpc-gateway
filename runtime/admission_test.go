package runtime_test

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const testAdmissionIdentity = "/test.Svc/Do"

func waitForAdmission(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

// admissionGate is an upstream stand-in: entered counts how many handler
// invocations ever started, active how many are inside the handler right now,
// and peak the highest value active ever reached. Every handler blocks on
// gate (or returns when the client goes away).
type admissionGate struct {
	entered int32
	active  int32
	peak    int32
	gate    chan struct{}
}

func (g *admissionGate) handler() runtime.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		atomic.AddInt32(&g.entered, 1)
		n := atomic.AddInt32(&g.active, 1)
		defer atomic.AddInt32(&g.active, -1)
		for {
			p := atomic.LoadInt32(&g.peak)
			if n <= p || atomic.CompareAndSwapInt32(&g.peak, p, n) {
				break
			}
		}
		select {
		case <-g.gate:
		case <-r.Context().Done():
			return
		}
		if r.Context().Err() != nil {
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}
}

func doRequest(mux *runtime.ServeMux, ctx context.Context) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	if ctx == nil {
		ctx = context.Background()
	}
	mux.ServeHTTP(rr, httptest.NewRequestWithContext(ctx, http.MethodGet, "/test", nil))
	return rr
}

// Without a declared rule requests bypass admission entirely: they are never
// queued, never rejected and run with unbounded concurrency.
func TestAdmission_NoLimit_PassThrough(t *testing.T) {
	barrier := make(chan struct{})
	up := &admissionGate{gate: make(chan struct{})}

	// Mux with a controller that has no rule for this identity: the generated
	// registration always goes through Admit, so this exercises that path.
	controller, err := runtime.NewAdmissionController(runtime.AdmissionRule{
		Service:       "other.Svc",
		MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatalf("NewAdmissionController: %v", err)
	}
	mux := runtime.NewServeMux(runtime.WithAdmissionController(controller))
	if err := mux.HandlePath(http.MethodGet, "/test", mux.Admit(testAdmissionIdentity, func(w http.ResponseWriter, r *http.Request, pp map[string]string) {
		<-barrier
		up.handler()(w, r, pp)
	})); err != nil {
		t.Fatalf("HandlePath: %v", err)
	}

	const n = 16
	var wg sync.WaitGroup
	recorders := make([]*httptest.ResponseRecorder, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		recorders[i] = httptest.NewRecorder()
		go func(rr *httptest.ResponseRecorder) {
			defer wg.Done()
			mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/test", nil))
		}(recorders[i])
	}
	close(barrier)
	waitForAdmission(t, 2*time.Second, func() bool { return atomic.LoadInt32(&up.entered) == n }, "all requests to enter the handler")
	if got := atomic.LoadInt32(&up.peak); got < 10 {
		t.Errorf("peak concurrent handlers = %d; want unbounded concurrency (>= 10)", got)
	}
	close(up.gate)
	wg.Wait()

	for i, rr := range recorders {
		if rr.Code != http.StatusOK {
			t.Errorf("request %d: code = %d; want 200", i, rr.Code)
		}
		if rr.Body.String() != "ok" {
			t.Errorf("request %d: body = %q; want %q", i, rr.Body.String(), "ok")
		}
		if rr.Header().Get("Retry-After") != "" {
			t.Errorf("request %d: unexpected Retry-After header without a declared rule", i)
		}
	}

	if running, waiting, ok := controller.AdmissionStats(testAdmissionIdentity); ok || running != 0 || waiting != 0 {
		t.Errorf("AdmissionStats for undeclared identity = %d,%d,%v; want 0,0,false", running, waiting, ok)
	}
}

// With MaxConcurrent slots and no queue, overflow is rejected with 429 before
// reaching the upstream; the number of handlers running never exceeds the
// declared value.
func TestAdmission_LimitAndImmediateRejection(t *testing.T) {
	up := &admissionGate{gate: make(chan struct{})}
	controller := runtime.MustAdmissionController(runtime.AdmissionRule{
		Service:       "test.Svc",
		MaxConcurrent: 2,
	})
	mux := runtime.NewServeMux(runtime.WithAdmissionController(controller))
	if err := mux.HandlePath(http.MethodGet, "/test", mux.Admit(testAdmissionIdentity, up.handler())); err != nil {
		t.Fatalf("HandlePath: %v", err)
	}

	done := make([]chan struct{}, 2)
	for i := range done {
		done[i] = make(chan struct{})
		go func(i int) {
			doRequest(mux, nil)
			close(done[i])
		}(i)
	}
	waitForAdmission(t, 2*time.Second, func() bool {
		running, _, _ := controller.AdmissionStats(testAdmissionIdentity)
		return running == 2
	}, "2 running requests")

	for i := 0; i < 4; i++ {
		rr := doRequest(mux, nil)
		if rr.Code != http.StatusTooManyRequests {
			t.Errorf("rejected request %d: code = %d; want 429", i, rr.Code)
		}
		if got := rr.Header().Get("Retry-After"); got != "1" {
			t.Errorf("rejected request %d: Retry-After = %q; want %q", i, got, "1")
		}
		if !strings.Contains(rr.Body.String(), `"code":8`) {
			t.Errorf("rejected request %d: body = %s; want ResourceExhausted status body", i, rr.Body.String())
		}
	}

	if got := atomic.LoadInt32(&up.entered); got != 2 {
		t.Errorf("upstream entered = %d; rejected requests must not reach the upstream, want 2", got)
	}
	if got := atomic.LoadInt32(&up.peak); got != 2 {
		t.Errorf("peak concurrent handlers = %d; want 2", got)
	}

	close(up.gate)
	for i, d := range done {
		select {
		case <-d:
		case <-time.After(2 * time.Second):
			t.Fatalf("running request %d did not finish after releasing the gate", i)
		}
	}
}

// When the queue has room, excess requests wait in arrival order; once the
// queue itself is full the request is rejected with 429.
func TestAdmission_QueuesAndDrains(t *testing.T) {
	up := &admissionGate{gate: make(chan struct{})}
	controller := runtime.MustAdmissionController(runtime.AdmissionRule{
		Service:       "test.Svc",
		MaxConcurrent: 1,
		MaxQueued:     2,
		QueueTimeout:  10 * time.Second,
	})
	mux := runtime.NewServeMux(runtime.WithAdmissionController(controller))
	if err := mux.HandlePath(http.MethodGet, "/test", mux.Admit(testAdmissionIdentity, up.handler())); err != nil {
		t.Fatalf("HandlePath: %v", err)
	}

	finished := make([]chan struct{}, 4)
	for i := range finished {
		finished[i] = make(chan struct{})
	}
	go func() { doRequest(mux, nil); close(finished[0]) }()
	waitForAdmission(t, time.Second, func() bool {
		running, _, _ := controller.AdmissionStats(testAdmissionIdentity)
		return running == 1
	}, "1 running request")

	go func() { doRequest(mux, nil); close(finished[1]) }()
	go func() { doRequest(mux, nil); close(finished[2]) }()
	waitForAdmission(t, 2*time.Second, func() bool {
		_, waiting, _ := controller.AdmissionStats(testAdmissionIdentity)
		return waiting == 2
	}, "2 waiting requests")

	rr := doRequest(mux, nil)
	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("queue-full request: code = %d; want 429", rr.Code)
	}
	if atomic.LoadInt32(&up.entered) != 1 {
		t.Errorf("upstream entered = %d; queued requests must not reach the upstream", up.entered)
	}

	close(up.gate)
	for i, f := range finished[:3] {
		select {
		case <-f:
		case <-time.After(2 * time.Second):
			t.Fatalf("request %d did not finish after draining", i)
		}
	}
	waitForAdmission(t, time.Second, func() bool {
		running, waiting, _ := controller.AdmissionStats(testAdmissionIdentity)
		return running == 0 && waiting == 0
	}, "all slots and the queue empty")
	if got, want := atomic.LoadInt32(&up.entered), int32(3); got != want {
		t.Errorf("upstream entered = %d; want %d served requests", got, want)
	}
}

// A queued request that cannot get a slot within QueueTimeout is rejected with
// 503 and never reaches the upstream.
func TestAdmission_QueueTimeout(t *testing.T) {
	up := &admissionGate{gate: make(chan struct{})}
	controller := runtime.MustAdmissionController(runtime.AdmissionRule{
		Service:       "test.Svc",
		MaxConcurrent: 1,
		MaxQueued:     3,
		QueueTimeout:  100 * time.Millisecond,
	})
	mux := runtime.NewServeMux(runtime.WithAdmissionController(controller))
	if err := mux.HandlePath(http.MethodGet, "/test", mux.Admit(testAdmissionIdentity, up.handler())); err != nil {
		t.Fatalf("HandlePath: %v", err)
	}

	go doRequest(mux, nil)
	waitForAdmission(t, time.Second, func() bool {
		running, _, _ := controller.AdmissionStats(testAdmissionIdentity)
		return running == 1
	}, "1 running request")

	start := time.Now()
	rrCh := make(chan *httptest.ResponseRecorder, 1)
	go func() { rrCh <- doRequest(mux, nil) }()
	var rr *httptest.ResponseRecorder
	select {
	case rr = <-rrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("queued request did not time out")
	}
	elapsed := time.Since(start)
	if elapsed < 80*time.Millisecond {
		t.Errorf("queued request returned after %v; want the QueueTimeout to elapse", elapsed)
	}
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("timed out request: code = %d; want 503", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "0" {
		t.Errorf("timed out request: Retry-After = %q; want %q", got, "0")
	}
	if !strings.Contains(rr.Body.String(), `"code":14`) {
		t.Errorf("timed out request: body = %s; want Unavailable status body", rr.Body.String())
	}
	if got := atomic.LoadInt32(&up.entered); got != 1 {
		t.Errorf("upstream entered = %d; a timed-out request must never reach the upstream", got)
	}
	waitForAdmission(t, time.Second, func() bool {
		_, waiting, _ := controller.AdmissionStats(testAdmissionIdentity)
		return waiting == 0
	}, "the timed out request to leave the queue")

	// The slot remains usable: release the running request, then a fresh one
	// is served.
	close(up.gate)
	waitForAdmission(t, time.Second, func() bool {
		running, waiting, _ := controller.AdmissionStats(testAdmissionIdentity)
		return running == 0 && waiting == 0
	}, "the running request to finish")
	rr2 := doRequest(mux, nil)
	if rr2.Code != http.StatusOK {
		t.Errorf("post-timeout request: code = %d; want 200", rr2.Code)
	}
}

// Canceling a queued request removes it from the queue without occupying a
// slot and without leaving a ghost waiter.
func TestAdmission_CancelQueuedRequest(t *testing.T) {
	up := &admissionGate{gate: make(chan struct{})}
	controller := runtime.MustAdmissionController(runtime.AdmissionRule{
		Service:       "test.Svc",
		MaxConcurrent: 1,
		MaxQueued:     3,
		QueueTimeout:  30 * time.Second,
	})
	mux := runtime.NewServeMux(runtime.WithAdmissionController(controller))
	if err := mux.HandlePath(http.MethodGet, "/test", mux.Admit(testAdmissionIdentity, up.handler())); err != nil {
		t.Fatalf("HandlePath: %v", err)
	}

	go doRequest(mux, nil)
	waitForAdmission(t, time.Second, func() bool {
		running, _, _ := controller.AdmissionStats(testAdmissionIdentity)
		return running == 1
	}, "1 running request")

	ctx, cancel := context.WithCancel(context.Background())
	queuedDone := make(chan struct{})
	go func() {
		doRequest(mux, ctx)
		close(queuedDone)
	}()
	waitForAdmission(t, time.Second, func() bool {
		_, waiting, _ := controller.AdmissionStats(testAdmissionIdentity)
		return waiting == 1
	}, "the canceled request to be queued")

	cancel()
	select {
	case <-queuedDone:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled queued request did not return")
	}
	waitForAdmission(t, time.Second, func() bool {
		_, waiting, _ := controller.AdmissionStats(testAdmissionIdentity)
		return waiting == 0
	}, "the canceled request to disappear from the queue")
	if got := atomic.LoadInt32(&up.entered); got != 1 {
		t.Errorf("upstream entered = %d; a canceled queued request must not reach the upstream", got)
	}

	close(up.gate)
	waitForAdmission(t, time.Second, func() bool {
		running, waiting, _ := controller.AdmissionStats(testAdmissionIdentity)
		return running == 0 && waiting == 0
	}, "the slot to be returned")
	if rr := doRequest(mux, nil); rr.Code != http.StatusOK {
		t.Errorf("request after queued cancel: code = %d; want 200 (no ghost waiter, no lost slot)", rr.Code)
	}
}

// Canceling a request that already holds a slot returns the slot immediately.
func TestAdmission_CancelRunningRequestReleasesSlot(t *testing.T) {
	release := make(chan struct{})
	up := &admissionGate{gate: release}
	controller := runtime.MustAdmissionController(runtime.AdmissionRule{
		Service:       "test.Svc",
		MaxConcurrent: 1,
	})
	mux := runtime.NewServeMux(runtime.WithAdmissionController(controller))
	if err := mux.HandlePath(http.MethodGet, "/test", mux.Admit(testAdmissionIdentity, up.handler())); err != nil {
		t.Fatalf("HandlePath: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go doRequest(mux, ctx)
	waitForAdmission(t, time.Second, func() bool {
		running, _, _ := controller.AdmissionStats(testAdmissionIdentity)
		return running == 1
	}, "1 running request")

	cancel()
	waitForAdmission(t, 2*time.Second, func() bool {
		running, _, _ := controller.AdmissionStats(testAdmissionIdentity)
		return running == 0
	}, "the canceled request to release its slot")

	close(release)
	rr := doRequest(mux, nil)
	if rr.Code != http.StatusOK {
		t.Errorf("request after running cancel: code = %d; want 200", rr.Code)
	}
}

// Queued requests receive slots strictly in arrival order.
func TestAdmission_FIFOOrder(t *testing.T) {
	const total = 6
	controller := runtime.MustAdmissionController(runtime.AdmissionRule{
		Service:       "test.Svc",
		MaxConcurrent: 1,
		MaxQueued:     total,
		QueueTimeout:  10 * time.Second,
	})
	mux := runtime.NewServeMux(runtime.WithAdmissionController(controller))

	gates := make([]chan struct{}, total)
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	var (
		mu         sync.Mutex
		entryOrder []int
	)
	if err := mux.HandlePath(http.MethodGet, "/test", mux.Admit(testAdmissionIdentity, func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		id, _ := strconv.Atoi(r.URL.Query().Get("id"))
		mu.Lock()
		entryOrder = append(entryOrder, id)
		mu.Unlock()
		<-gates[id]
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, strconv.Itoa(id))
	})); err != nil {
		t.Fatalf("HandlePath: %v", err)
	}

	done := make([]chan int, total)
	for i := range done {
		done[i] = make(chan int, 1)
	}
	dispatch := func(id int) {
		go func() {
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/test?id="+strconv.Itoa(id), nil))
			done[id] <- rr.Code
		}()
	}

	dispatch(0)
	waitForAdmission(t, time.Second, func() bool {
		running, _, _ := controller.AdmissionStats(testAdmissionIdentity)
		return running == 1
	}, "request 0 to be running")

	for id := 1; id < total; id++ {
		dispatch(id)
		time.Sleep(5 * time.Millisecond)
	}
	waitForAdmission(t, 2*time.Second, func() bool {
		_, waiting, _ := controller.AdmissionStats(testAdmissionIdentity)
		return waiting == total-1
	}, "all other requests to be queued")

	var servedOrder []int
	for id := 0; id < total; id++ {
		close(gates[id])
		select {
		case code := <-done[id]:
			if code != http.StatusOK {
				t.Fatalf("request %d: code = %d; want 200", id, code)
			}
			servedOrder = append(servedOrder, id)
		case <-time.After(2 * time.Second):
			t.Fatalf("request %d did not finish in FIFO turn", id)
		}
	}
	for i, id := range servedOrder {
		if id != i {
			t.Errorf("serve order = %v; want %v", servedOrder, []int{0, 1, 2, 3, 4, 5})
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for i, id := range entryOrder {
		if id != i {
			t.Errorf("upstream entry order = %v; want %v", entryOrder, []int{0, 1, 2, 3, 4, 5})
		}
	}
}

// A streaming request occupies a single slot from the moment the upstream
// stream starts until the stream ends; a disconnecting client frees it.
func TestAdmission_StreamingSingleSlotAndDisconnect(t *testing.T) {
	var entered int32
	controller := runtime.MustAdmissionController(runtime.AdmissionRule{
		Service:       "test.Stream",
		MaxConcurrent: 1,
		MaxQueued:     5,
		QueueTimeout:  5 * time.Second,
	})
	mux := runtime.NewServeMux(runtime.WithAdmissionController(controller))
	if err := mux.HandlePath(http.MethodGet, "/stream", mux.Admit("/test.Stream/Send", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		atomic.AddInt32(&entered, 1)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Errorf("ResponseWriter is not a Flusher")
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				if _, err := io.WriteString(w, "x\n"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	})); err != nil {
		t.Fatalf("HandlePath: %v", err)
	}

	ts := httptest.NewServer(mux)
	defer ts.Close()
	client := &http.Client{}

	readToken := func(t *testing.T, body io.Reader) error {
		t.Helper()
		br := bufio.NewReader(body)
		_, err := br.ReadString('\n')
		return err
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	reqA, _ := http.NewRequestWithContext(ctxA, http.MethodGet, ts.URL+"/stream", nil)
	respA, err := client.Do(reqA)
	if err != nil {
		t.Fatalf("client A Do: %v", err)
	}
	defer respA.Body.Close()
	if err := readToken(t, respA.Body); err != nil {
		t.Fatalf("client A read: %v", err)
	}
	waitForAdmission(t, time.Second, func() bool {
		running, _, _ := controller.AdmissionStats("/test.Stream/Send")
		return running == 1
	}, "client A to hold the slot")

	// Client B is queued: its response headers (and therefore client.Do) must
	// not arrive while A holds the slot, and B's upstream handler must not run.
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	reqB, _ := http.NewRequestWithContext(ctxB, http.MethodGet, ts.URL+"/stream", nil)
	bGotHeaders := make(chan struct{})
	var respB *http.Response
	go func() {
		resp, err := client.Do(reqB)
		if err == nil {
			// Keep the stream open until the test cancels ctxB; closing the
			// body here would make the server release B's slot immediately.
			respB = resp
		}
		close(bGotHeaders)
	}()
	waitForAdmission(t, time.Second, func() bool {
		_, waiting, _ := controller.AdmissionStats("/test.Stream/Send")
		return waiting == 1
	}, "client B to be queued")
	select {
	case <-bGotHeaders:
		t.Fatal("queued client B received response headers while client A held the only slot")
	case <-time.After(400 * time.Millisecond):
	}
	if got := atomic.LoadInt32(&entered); got != 1 {
		t.Errorf("upstream streams started = %d; queued client B must not reach the upstream", got)
	}

	cancelA()
	select {
	case <-bGotHeaders:
	case <-time.After(2 * time.Second):
		t.Fatal("client B did not get the slot after client A disconnected")
	}
	waitForAdmission(t, time.Second, func() bool {
		running, _, _ := controller.AdmissionStats("/test.Stream/Send")
		return running == 1
	}, "client B to hold the slot after transfer")
	if got := atomic.LoadInt32(&entered); got != 2 {
		t.Errorf("upstream streams started = %d; want 2 after slot transfer", got)
	}

	cancelB()
	if respB != nil {
		respB.Body.Close()
	}
	waitForAdmission(t, 2*time.Second, func() bool {
		running, waiting, _ := controller.AdmissionStats("/test.Stream/Send")
		return running == 0 && waiting == 0
	}, "client B disconnect to release the slot")

	respC, err := client.Get(ts.URL + "/stream")
	if err != nil {
		t.Fatalf("client C Do: %v", err)
	}
	if err := readToken(t, respC.Body); err != nil {
		t.Fatalf("client C read: %v", err)
	}
	waitForAdmission(t, time.Second, func() bool {
		running, _, _ := controller.AdmissionStats("/test.Stream/Send")
		return running == 1
	}, "client C to hold the slot")
	respC.Body.Close()
	waitForAdmission(t, 2*time.Second, func() bool {
		running, _, _ := controller.AdmissionStats("/test.Stream/Send")
		return running == 0
	}, "client C closing the body to release the slot")
}

// A method-specific rule overrides the service-wide rule; siblings still use
// the service-wide one.
func TestAdmission_MethodRuleOverridesServiceRule(t *testing.T) {
	serviceUp := &admissionGate{gate: make(chan struct{})}
	methodUp := &admissionGate{gate: make(chan struct{})}
	controller := runtime.MustAdmissionController(
		runtime.AdmissionRule{Service: "test.Svc", MaxConcurrent: 1},
		runtime.AdmissionRule{Service: "test.Svc", Method: "Wide", MaxConcurrent: 5},
	)
	mux := runtime.NewServeMux(runtime.WithAdmissionController(controller))
	if err := mux.HandlePath(http.MethodGet, "/other", mux.Admit("/test.Svc/Other", serviceUp.handler())); err != nil {
		t.Fatalf("HandlePath /other: %v", err)
	}
	if err := mux.HandlePath(http.MethodGet, "/wide", mux.Admit("/test.Svc/Wide", methodUp.handler())); err != nil {
		t.Fatalf("HandlePath /wide: %v", err)
	}

	// Occupy the service-wide limiter through the sibling method.
	go doRequestWithPath(mux, "/other", nil)
	waitForAdmission(t, time.Second, func() bool {
		running, _, _ := controller.AdmissionStats("/test.Svc/Other")
		return running == 1
	}, "the sibling to occupy the service-wide slot")
	if rr := doRequestWithPath(mux, "/other", nil); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("second sibling: code = %d; want 429 from the service rule", rr.Code)
	}

	// The method-specific limiter is independent and allows 5 in parallel even
	// while the service-wide slot is busy.
	done := make([]chan struct{}, 5)
	for i := range done {
		done[i] = make(chan struct{})
		go func(i int) {
			doRequestWithPath(mux, "/wide", nil)
			close(done[i])
		}(i)
	}
	waitForAdmission(t, 2*time.Second, func() bool {
		running, _, _ := controller.AdmissionStats("/test.Svc/Wide")
		return running == 5
	}, "5 method-rule requests to run")

	close(methodUp.gate)
	for i := range done {
		<-done[i]
	}
	close(serviceUp.gate)
}

func doRequestWithPath(mux *runtime.ServeMux, path string, ctx context.Context) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	if ctx == nil {
		ctx = context.Background()
	}
	mux.ServeHTTP(rr, httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil))
	return rr
}

// Building a controller is all-or-nothing: an invalid rule aborts the build
// instead of leaving a half-built rule table.
func TestAdmissionController_InvalidRules(t *testing.T) {
	valid := runtime.AdmissionRule{Service: "a.Svc", MaxConcurrent: 1}
	for i, tc := range []struct {
		name string
		rule runtime.AdmissionRule
	}{
		{"empty service", runtime.AdmissionRule{MaxConcurrent: 1}},
		{"service with leading slash", runtime.AdmissionRule{Service: "/a.Svc", MaxConcurrent: 1}},
		{"zero concurrency", runtime.AdmissionRule{Service: "b.Svc", MaxConcurrent: 0}},
		{"negative concurrency", runtime.AdmissionRule{Service: "b.Svc", MaxConcurrent: -1}},
		{"negative queue", runtime.AdmissionRule{Service: "b.Svc", MaxConcurrent: 1, MaxQueued: -1}},
		{"queue without timeout", runtime.AdmissionRule{Service: "b.Svc", MaxConcurrent: 1, MaxQueued: 1}},
		{"method with slash", runtime.AdmissionRule{Service: "b.Svc", Method: "a/b", MaxConcurrent: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A duplicate of the valid rule proves the bad rule is the cause.
			rules := []runtime.AdmissionRule{valid, tc.rule}
			if ac, err := runtime.NewAdmissionController(rules...); err == nil {
				t.Fatalf("NewAdmissionController case %d: want error, got controller %v", i, ac)
			}
		})
	}

	if _, err := runtime.NewAdmissionController(
		runtime.AdmissionRule{Service: "dup.Svc", MaxConcurrent: 1},
		runtime.AdmissionRule{Service: "dup.Svc", MaxConcurrent: 2},
	); err == nil {
		t.Fatal("duplicate service rule: want error")
	}
	if _, err := runtime.NewAdmissionController(
		runtime.AdmissionRule{Service: "dup.Svc", Method: "M", MaxConcurrent: 1},
		runtime.AdmissionRule{Service: "dup.Svc", Method: "M", MaxConcurrent: 2},
	); err == nil {
		t.Fatal("duplicate method rule: want error")
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("MustAdmissionController with invalid rules: want panic")
			}
		}()
		runtime.MustAdmissionController(runtime.AdmissionRule{Service: "x.Svc", MaxConcurrent: 0})
	}()
}

// Middlewares still wrap the whole chain: they observe both accepted and
// rejected requests, in the same order as before.
func TestAdmission_MiddlewareWrapsAdmission(t *testing.T) {
	up := &admissionGate{gate: make(chan struct{})}
	mw := func(n int) runtime.Middleware {
		return func(next runtime.HandlerFunc) runtime.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request, pp map[string]string) {
				w.Header().Set("X-Mw-"+strconv.Itoa(n), "seen")
				next(w, r, pp)
			}
		}
	}
	mux := runtime.NewServeMux(
		runtime.WithMiddlewares(mw(1), mw(2)),
		runtime.WithAdmissionRules(runtime.AdmissionRule{Service: "test.Svc", MaxConcurrent: 1}),
	)
	if err := mux.HandlePath(http.MethodGet, "/test", mux.Admit(testAdmissionIdentity, up.handler())); err != nil {
		t.Fatalf("HandlePath: %v", err)
	}

	go doRequest(mux, nil)
	waitForAdmission(t, time.Second, func() bool { return atomic.LoadInt32(&up.entered) == 1 }, "1 running request")

	rr := doRequest(mux, nil)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("code = %d; want 429", rr.Code)
	}
	if rr.Header().Get("X-Mw-1") != "seen" || rr.Header().Get("X-Mw-2") != "seen" {
		t.Errorf("middleware headers on rejected request = %v; want both middlewares to run", rr.Header())
	}
	close(up.gate)
}

// The health check endpoint goes through the same admission as generated
// routes: it carries the grpc.health.v1.Health/Check rule identity.
func TestAdmission_HealthEndpointUsesSameAdmission(t *testing.T) {
	client := &blockingHealthCheckClient{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	mux := runtime.NewServeMux(
		runtime.WithHealthEndpointAt(client, "/healthz"),
		runtime.WithAdmissionRules(runtime.AdmissionRule{
			Service:       "grpc.health.v1.Health",
			MaxConcurrent: 1,
		}),
	)

	go func() {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	}()
	select {
	case <-client.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first health check did not reach the health client")
	}

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("second health check: code = %d; want 429", rr.Code)
	}
	if rr.Header().Get("Retry-After") != "1" {
		t.Errorf("second health check: Retry-After = %q; want %q", rr.Header().Get("Retry-After"), "1")
	}

	close(client.release)
}

type blockingHealthCheckClient struct {
	entered chan struct{}
	release chan struct{}
}

func (c *blockingHealthCheckClient) Check(ctx context.Context, _ *healthpb.HealthCheckRequest, _ ...grpc.CallOption) (*healthpb.HealthCheckResponse, error) {
	c.entered <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, status.Error(codes.Canceled, "canceled")
	case <-c.release:
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
	}
}

func (c *blockingHealthCheckClient) Watch(context.Context, *healthpb.HealthCheckRequest, ...grpc.CallOption) (healthpb.Health_WatchClient, error) {
	return nil, status.Error(codes.Unimplemented, "unimplemented")
}

func (c *blockingHealthCheckClient) List(context.Context, *healthpb.HealthListRequest, ...grpc.CallOption) (*healthpb.HealthListResponse, error) {
	return nil, status.Error(codes.Unimplemented, "unimplemented")
}
