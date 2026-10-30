package runtime_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	pb "github.com/grpc-ecosystem/grpc-gateway/v2/runtime/internal/examplepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// framingExpectation records the two framing conclusions the acceptance
// criteria care about.
type framingExpectation struct {
	chunked       bool
	contentLength string // "", "set", or "exact:<n>" is checked against the body
}

// driveUnary runs one buffered (non-streaming) response through ForwardResponseMessage.
func driveUnary(t *testing.T, mux *runtime.ServeMux, acceptTrailers bool) *httptest.ResponseRecorder {
	t.Helper()
	var md runtime.ServerMetadata
	if acceptTrailers {
		md.TrailerMD = metadata.Pairs("foo", "bar")
	}
	ctx := runtime.NewServerMetadataContext(context.Background(), md)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/foo", nil)
	if acceptTrailers {
		req.Header.Set("TE", "trailers")
	}
	w := httptest.NewRecorder()
	runtime.ForwardResponseMessage(ctx, mux, &runtime.JSONPb{}, w, req, &pb.SimpleMessage{Id: "One"})
	return w
}

// driveStream runs one server-streaming response through ForwardResponseStream.
func driveStream(t *testing.T, mux *runtime.ServeMux) *httptest.ResponseRecorder {
	t.Helper()
	ctx := runtime.NewServerMetadataContext(context.Background(), runtime.ServerMetadata{})
	req := httptest.NewRequest(http.MethodGet, "http://example.com/foo", nil)
	w := httptest.NewRecorder()
	var calls int
	recv := func() (proto.Message, error) {
		calls++
		if calls == 1 {
			return &pb.SimpleMessage{Id: "One"}, nil
		}
		return nil, io.EOF
	}
	runtime.ForwardResponseStream(ctx, mux, &runtime.JSONPb{}, w, req, recv)
	return w
}

// driveError runs one buffered response through the default error handler.
func driveError(t *testing.T, mux *runtime.ServeMux, acceptTrailers bool) *httptest.ResponseRecorder {
	t.Helper()
	var md runtime.ServerMetadata
	if acceptTrailers {
		md.TrailerMD = metadata.Pairs("foo", "bar")
	}
	ctx := runtime.NewServerMetadataContext(context.Background(), md)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/foo", nil)
	if acceptTrailers {
		req.Header.Set("TE", "trailers")
	}
	w := httptest.NewRecorder()
	runtime.DefaultHTTPErrorHandler(ctx, mux, &runtime.JSONPb{}, w, req, status.Error(codes.NotFound, "missing"))
	return w
}

func assertFraming(t *testing.T, name string, w *httptest.ResponseRecorder, want framingExpectation, checkLengthBody bool) {
	t.Helper()
	result := w.Result()
	defer result.Body.Close()
	gotChunked := result.Header.Get("Transfer-Encoding") == "chunked"
	if gotChunked != want.chunked {
		t.Errorf("%s: Transfer-Encoding chunked = %v; want %v (headers=%v)", name, gotChunked, want.chunked, result.Header)
	}
	switch {
	case want.contentLength == "":
		if got := result.Header.Get("Content-Length"); got != "" {
			t.Errorf("%s: Content-Length = %q; want none", name, got)
		}
	case want.contentLength == "set":
		got := result.Header.Get("Content-Length")
		if got == "" {
			t.Fatalf("%s: Content-Length is missing; want it set", name)
		}
		if checkLengthBody {
			body, err := io.ReadAll(result.Body)
			if err != nil {
				t.Fatalf("%s: read body: %v", name, err)
			}
			if n, err := strconv.Atoi(got); err != nil || n != len(body) {
				t.Errorf("%s: Content-Length = %q; want %d (actual body length)", name, got, len(body))
			}
		}
	}
}

// TestFramingPolicyConsistentAcrossPaths checks the acceptance criterion:
// with the same set of switches, the unary, streaming and error paths each
// reach the chunked/length conclusion dictated by the one resolved policy.
func TestFramingPolicyConsistentAcrossPaths(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opts []runtime.ServeMuxOption
		// expectations indexed by scenario
		unary            framingExpectation
		unaryWithTrailer framingExpectation
		stream           framingExpectation
		err              framingExpectation
		errWithTrailer   framingExpectation
	}{
		{
			name:             "no switches",
			unary:            framingExpectation{chunked: false, contentLength: ""},
			unaryWithTrailer: framingExpectation{chunked: true, contentLength: ""},
			stream:           framingExpectation{chunked: true, contentLength: ""},
			err:              framingExpectation{chunked: false, contentLength: ""},
			errWithTrailer:   framingExpectation{chunked: true, contentLength: ""},
		},
		{
			name:             "disable chunked encoding",
			opts:             []runtime.ServeMuxOption{runtime.WithDisableChunkedEncoding()},
			unary:            framingExpectation{chunked: false},
			unaryWithTrailer: framingExpectation{chunked: false},
			stream:           framingExpectation{chunked: false},
			err:              framingExpectation{chunked: false},
			errWithTrailer:   framingExpectation{chunked: false},
		},
		{
			name:             "write content length",
			opts:             []runtime.ServeMuxOption{runtime.WithWriteContentLength()},
			unary:            framingExpectation{chunked: false, contentLength: "set"},
			unaryWithTrailer: framingExpectation{chunked: true, contentLength: ""},
			stream:           framingExpectation{chunked: true, contentLength: ""},
			err:              framingExpectation{chunked: false, contentLength: "set"},
			errWithTrailer:   framingExpectation{chunked: true, contentLength: ""},
		},
		{
			name:             "disable chunked and write content length",
			opts:             []runtime.ServeMuxOption{runtime.WithDisableChunkedEncoding(), runtime.WithWriteContentLength()},
			unary:            framingExpectation{chunked: false, contentLength: "set"},
			unaryWithTrailer: framingExpectation{chunked: false, contentLength: ""},
			stream:           framingExpectation{chunked: false, contentLength: ""},
			err:              framingExpectation{chunked: false, contentLength: "set"},
			errWithTrailer:   framingExpectation{chunked: false, contentLength: ""},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Build the mux twice with the options in opposite declaration
			// order; both must behave identically.
			for _, reverse := range []bool{false, true} {
				opts := append([]runtime.ServeMuxOption(nil), tc.opts...)
				if reverse {
					for i, j := 0, len(opts)-1; i < j; i, j = i+1, j-1 {
						opts[i], opts[j] = opts[j], opts[i]
					}
				}
				mux := runtime.NewServeMux(opts...)
				suffix := ""
				if reverse {
					suffix = " (reversed order)"
				}
				assertFraming(t, "unary"+suffix, driveUnary(t, mux, false), tc.unary, true)
				assertFraming(t, "unary-trailers"+suffix, driveUnary(t, mux, true), tc.unaryWithTrailer, false)
				assertFraming(t, "stream"+suffix, driveStream(t, mux), tc.stream, false)
				assertFraming(t, "error"+suffix, driveError(t, mux, false), tc.err, true)
				assertFraming(t, "error-trailers"+suffix, driveError(t, mux, true), tc.errWithTrailer, false)
			}
		})
	}
}

func newRoutingMux(t *testing.T, opts []runtime.ServeMuxOption, methods ...string) *runtime.ServeMux {
	t.Helper()
	mux := runtime.NewServeMux(opts...)
	for _, method := range methods {
		method := method
		if err := mux.HandlePath(method, "/foo", func(w http.ResponseWriter, _ *http.Request, _ map[string]string) {
			_, _ = fmt.Fprintf(w, "%s /foo", method)
		}); err != nil {
			t.Fatalf("HandlePath(%q): %v", method, err)
		}
	}
	return mux
}

func formPost(override string, contentType string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "http://example.com/foo", strings.NewReader("a=b"))
	if contentType == "" {
		contentType = "application/x-www-form-urlencoded"
	}
	r.Header.Set("Content-Type", contentType)
	if override != "" {
		r.Header.Set("X-HTTP-Method-Override", override)
	}
	return r
}

// TestDisablePathLengthFallbackKeepsMethodOverride verifies that disabling
// the path-length fallback no longer silently disables the method override.
func TestDisablePathLengthFallbackKeepsMethodOverride(t *testing.T) {
	t.Parallel()

	// Fallback disabled, only a GET handler: the explicit override still
	// routes the form POST to the GET handler.
	mux := newRoutingMux(t, []runtime.ServeMuxOption{runtime.WithDisablePathLengthFallback()}, http.MethodGet)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, formPost("GET", ""))
	if w.Code != http.StatusOK || w.Body.String() != "GET /foo" {
		t.Fatalf("override with fallback disabled: code=%d body=%q; want 200 GET /foo", w.Code, w.Body.String())
	}

	// Same mux, no override header: the implicit fallback stays disabled, so
	// the POST must not silently reach the GET handler. The GET pattern still
	// exists, so the mux answers 405 (mapped to 501) instead of falling back.
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, formPost("", ""))
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("fallback disabled without override: code=%d; want 501", w.Code)
	}

	// Both switches off: the override is deterministically unavailable too.
	both := newRoutingMux(t,
		[]runtime.ServeMuxOption{runtime.WithDisablePathLengthFallback(), runtime.WithDisableHTTPMethodOverride()},
		http.MethodGet)
	w = httptest.NewRecorder()
	both.ServeHTTP(w, formPost("GET", ""))
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("both disabled: code=%d; want 501 (override ignored)", w.Code)
	}

	// The override applies to form POSTs only: a JSON POST with the header is
	// not rerouted even while the override switch remains enabled.
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, formPost("GET", "application/json"))
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("JSON POST override: code=%d; want 501", w.Code)
	}

	// Defaults untouched: form POST without override falls back to GET.
	def := newRoutingMux(t, nil, http.MethodGet)
	w = httptest.NewRecorder()
	def.ServeHTTP(w, formPost("", ""))
	if w.Code != http.StatusOK || w.Body.String() != "GET /foo" {
		t.Fatalf("default fallback: code=%d body=%q; want 200 GET /foo", w.Code, w.Body.String())
	}
}

// TestServeMux_ConcurrentRequestsShareSnapshot hammers one resolved mux; run
// with -race to verify the frozen snapshot is safe to share.
func TestServeMux_ConcurrentRequestsShareSnapshot(t *testing.T) {
	t.Parallel()

	mux := runtime.NewServeMux(
		runtime.WithDisableChunkedEncoding(),
		runtime.WithWriteContentLength(),
		runtime.WithDisablePathLengthFallback(),
	)
	if err := mux.HandlePath(http.MethodGet, "/foo", func(w http.ResponseWriter, _ *http.Request, _ map[string]string) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodGet, "/foo", nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Errorf("request %d: code=%d; want 200", i, w.Code)
			}
		}(i)
	}
	wg.Wait()
}
