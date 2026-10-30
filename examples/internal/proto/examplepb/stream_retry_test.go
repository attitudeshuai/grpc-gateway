package examplepb_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/examples/internal/proto/examplepb"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// streamListServer opens exactly one server stream per request. When fail is
// set it fails that stream immediately; otherwise it sends no messages and
// closes cleanly.
type streamListServer struct {
	examplepb.UnimplementedStreamServiceServer
	listCalls atomic.Int32
	fail      bool
}

func (s *streamListServer) List(_ *examplepb.Options, srv grpc.ServerStreamingServer[examplepb.ABitOfEverything]) error {
	s.listCalls.Add(1)
	if s.fail {
		return status.Error(codes.Unavailable, "stream down")
	}
	return nil
}

// TestStreamingMethodsAreNeverRetried verifies that server-streaming methods
// hit the upstream exactly once even when a retry policy is declared for the
// service. Retry only applies to unary calls.
func TestStreamingMethodsAreNeverRetried(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream := grpc.NewServer()
	flaky := &streamListServer{fail: true}
	examplepb.RegisterStreamServiceServer(upstream, flaky)
	go func() { _ = upstream.Serve(lis) }()
	t.Cleanup(upstream.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := examplepb.NewStreamServiceClient(conn)

	observed := make(chan runtime.RetryStats, 1)
	mux := runtime.NewServeMux(
		runtime.WithRetryPolicy(examplepb.StreamService_List_FullMethodName, runtime.RetryPolicy{
			MaxAttempts: 5,
		}),
		// A service-wide declaration must be ignored by streaming methods too.
		runtime.WithRetryPolicy("/grpc.gateway.examples.internal.proto.examplepb.StreamService", runtime.RetryPolicy{
			MaxAttempts: 5,
			Backoff:     noBackoff{},
		}),
		runtime.WithRetryObserver(func(stats runtime.RetryStats) {
			select {
			case observed <- stats:
			default:
			}
		}),
	)
	if err := examplepb.RegisterStreamServiceHandlerClient(context.Background(), mux, client); err != nil {
		t.Fatal(err)
	}

	gw := httptest.NewServer(mux)
	t.Cleanup(gw.Close)

	resp, err := http.Get(gw.URL + "/v1/example/a_bit_of_everything")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body: %s", resp.StatusCode, body)
	}
	if got := flaky.listCalls.Load(); got != 1 {
		t.Errorf("upstream stream opened %d times, want exactly 1", got)
	}
	select {
	case stats := <-observed:
		t.Errorf("retry observer must not fire for streaming methods, got %+v", stats)
	default:
	}
}

// TestStreamingMethodsHappyPathOpensOnce verifies a successful stream also
// opens exactly once and is forwarded as an empty 200 response.
func TestStreamingMethodsHappyPathOpensOnce(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream := grpc.NewServer()
	srv := &streamListServer{fail: false}
	examplepb.RegisterStreamServiceServer(upstream, srv)
	go func() { _ = upstream.Serve(lis) }()
	t.Cleanup(upstream.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := examplepb.NewStreamServiceClient(conn)

	mux := runtime.NewServeMux(
		runtime.WithRetryPolicy("/grpc.gateway.examples.internal.proto.examplepb.StreamService", runtime.RetryPolicy{
			MaxAttempts: 5,
		}),
	)
	if err := examplepb.RegisterStreamServiceHandlerClient(context.Background(), mux, client); err != nil {
		t.Fatal(err)
	}

	gw := httptest.NewServer(mux)
	t.Cleanup(gw.Close)

	resp, err := http.Get(gw.URL + "/v1/example/a_bit_of_everything")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := srv.listCalls.Load(); got != 1 {
		t.Errorf("upstream stream opened %d times, want exactly 1", got)
	}
}

type noBackoff struct{}

func (noBackoff) NextBackoff(int) time.Duration { return 0 }
