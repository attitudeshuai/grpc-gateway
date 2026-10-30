package helloworld_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/examples/internal/helloworld"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// slowGreeterServer blocks SayHello on a gate so tests can pin exactly one
// in-flight upstream call.
type slowGreeterServer struct {
	helloworld.UnimplementedGreeterServer
	entered int32
	active  int32
	peak    int32
	gate    chan struct{}
}

func (s *slowGreeterServer) SayHello(ctx context.Context, _ *helloworld.HelloRequest) (*helloworld.HelloReply, error) {
	atomic.AddInt32(&s.entered, 1)
	n := atomic.AddInt32(&s.active, 1)
	defer atomic.AddInt32(&s.active, -1)
	for {
		p := atomic.LoadInt32(&s.peak)
		if n <= p || atomic.CompareAndSwapInt32(&s.peak, p, n) {
			break
		}
	}
	select {
	case <-s.gate:
		return &helloworld.HelloReply{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Exercises the full generated registration path (mux.Admit identity emitted
// by protoc-gen-grpc-gateway) against a real gRPC upstream: with one slot and
// one queued slot, the third concurrent request is rejected with 429 before
// reaching the upstream, the queued request is served after the first one
// finishes, and the upstream never sees more than one concurrent call.
func TestGeneratedHandler_AdmissionConcurrencyAndQueue(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	upstream := &slowGreeterServer{gate: make(chan struct{})}
	grpcServer := grpc.NewServer()
	helloworld.RegisterGreeterServer(grpcServer, upstream)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	const fullMethod = "/grpc.gateway.examples.internal.helloworld.Greeter/SayHello"
	controller := gwruntime.MustAdmissionController(gwruntime.AdmissionRule{
		Service:       "grpc.gateway.examples.internal.helloworld.Greeter",
		MaxConcurrent: 1,
		MaxQueued:     1,
		QueueTimeout:  10 * time.Second,
	})
	mux := gwruntime.NewServeMux(gwruntime.WithAdmissionController(controller))
	if err := helloworld.RegisterGreeterHandlerClient(context.Background(), mux, helloworld.NewGreeterClient(conn)); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	ts := httptest.NewServer(mux)
	defer ts.Close()

	get := func() (int, string) {
		resp, err := http.Get(ts.URL + "/say/world")
		if err != nil {
			t.Fatalf("http.Get: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	aDone := make(chan int, 1)
	go func() { code, _ := get(); aDone <- code }()
	waitFor(t, 2*time.Second, func() bool {
		running, _, _ := controller.AdmissionStats(fullMethod)
		return running == 1
	}, "request A to run against the upstream")

	bDone := make(chan int, 1)
	go func() { code, _ := get(); bDone <- code }()
	waitFor(t, 2*time.Second, func() bool {
		_, waiting, _ := controller.AdmissionStats(fullMethod)
		return waiting == 1
	}, "request B to be queued")

	code, body := get()
	if code != http.StatusTooManyRequests {
		t.Errorf("request C: status = %d; want 429", code)
	}
	if got := atomic.LoadInt32(&upstream.entered); got != 1 {
		t.Errorf("upstream calls started = %d; rejected request C must never reach the upstream", got)
	}
	if !strings.Contains(body, `"code":8`) {
		t.Errorf("request C: body = %q; want a ResourceExhausted error body", body)
	}

	close(upstream.gate)

	for name, done := range map[string]chan int{"A": aDone, "B": bDone} {
		select {
		case got := <-done:
			if got != http.StatusOK {
				t.Errorf("request %s: status = %d; want 200", name, got)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("request %s did not finish after releasing the gate", name)
		}
	}

	if got, want := atomic.LoadInt32(&upstream.peak), int32(1); got != want {
		t.Errorf("upstream peak concurrency = %d; want %d", got, want)
	}
	if got, want := atomic.LoadInt32(&upstream.entered), int32(2); got != want {
		t.Errorf("upstream calls started = %d; want %d (A and queued B)", got, want)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
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
