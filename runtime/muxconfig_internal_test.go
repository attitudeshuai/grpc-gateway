package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// resolvedView is the order-independent part of the resolved snapshot. Hook
// functions are deliberately excluded: function values are not comparable, so
// the single-valued hook slots have their own conflict tests.
type resolvedView struct {
	disableChunkedEncoding    bool
	writeContentLength        bool
	disablePathLengthFallback bool
	disableHTTPMethodOverride bool
	unescapingMode            UnescapingMode
	strictDeepWildcard        bool
}

func viewOf(c *muxConfig) resolvedView {
	return resolvedView{
		disableChunkedEncoding:    c.disableChunkedEncoding,
		writeContentLength:        c.writeContentLength,
		disablePathLengthFallback: c.disablePathLengthFallback,
		disableHTTPMethodOverride: c.disableHTTPMethodOverride,
		unescapingMode:            c.unescapingMode,
		strictDeepWildcard:        c.strictDeepWildcard,
	}
}

func recoverNewServeMuxPanic(t *testing.T, opts ...ServeMuxOption) (recovered any) {
	t.Helper()
	defer func() {
		recovered = recover()
	}()
	_ = NewServeMux(opts...)
	return recovered
}

func assertPanicNamesBoth(t *testing.T, name string, got any, first, second string) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: NewServeMux did not panic; want a construction failure naming both switches", name)
	}
	err, ok := got.(error)
	if !ok {
		t.Fatalf("%s: panicked with %T (%v); want an error", name, got, got)
	}
	msg := err.Error()
	if !strings.Contains(msg, first) || !strings.Contains(msg, second) {
		t.Fatalf("%s: panic message %q does not name both %q and %q", name, msg, first, second)
	}
}

func noopErrorHandler(context.Context, *ServeMux, Marshaler, http.ResponseWriter, *http.Request, error) {
}

func TestNewServeMux_ConflictingScalarOptionsAreRejected(t *testing.T) {
	t.Parallel()

	// Each case is a combination that cannot take effect as declared: two
	// single-valued options fight for one slot.
	t.Run("two different unescaping modes", func(t *testing.T) {
		t.Parallel()
		got := recoverNewServeMuxPanic(t,
			WithUnescapingMode(UnescapingModeAllCharacters),
			WithUnescapingMode(UnescapingModeLegacy),
		)
		assertPanicNamesBoth(t, "unescaping mode", got,
			"WithUnescapingMode(UnescapingModeAllCharacters)",
			"WithUnescapingMode(UnescapingModeLegacy)")
	})

	t.Run("two error handlers", func(t *testing.T) {
		t.Parallel()
		got := recoverNewServeMuxPanic(t,
			WithErrorHandler(DefaultHTTPErrorHandler),
			WithErrorHandler(noopErrorHandler),
		)
		assertPanicNamesBoth(t, "error handler", got, "WithErrorHandler", "WithErrorHandler")
	})

	t.Run("two stream error handlers", func(t *testing.T) {
		t.Parallel()
		got := recoverNewServeMuxPanic(t,
			WithStreamErrorHandler(DefaultStreamErrorHandler),
			WithStreamErrorHandler(func(context.Context, error) *status.Status {
				return status.New(codes.Internal, "x")
			}),
		)
		assertPanicNamesBoth(t, "stream error handler", got, "WithStreamErrorHandler", "WithStreamErrorHandler")
	})

	t.Run("two routing error handlers", func(t *testing.T) {
		t.Parallel()
		got := recoverNewServeMuxPanic(t,
			WithRoutingErrorHandler(DefaultRoutingErrorHandler),
			WithRoutingErrorHandler(DefaultRoutingErrorHandler),
		)
		assertPanicNamesBoth(t, "routing error handler", got, "WithRoutingErrorHandler", "WithRoutingErrorHandler")
	})

	t.Run("two forward response rewriters", func(t *testing.T) {
		t.Parallel()
		rewriter := func(_ context.Context, _ proto.Message) (any, error) { return nil, nil }
		got := recoverNewServeMuxPanic(t,
			WithForwardResponseRewriter(rewriter),
			WithForwardResponseRewriter(rewriter),
		)
		assertPanicNamesBoth(t, "forward response rewriter", got,
			"WithForwardResponseRewriter", "WithForwardResponseRewriter")
	})

	t.Run("two incoming header matchers", func(t *testing.T) {
		t.Parallel()
		matcher := func(string) (string, bool) { return "", false }
		got := recoverNewServeMuxPanic(t,
			WithIncomingHeaderMatcher(matcher),
			WithIncomingHeaderMatcher(matcher),
		)
		assertPanicNamesBoth(t, "incoming header matcher", got,
			"WithIncomingHeaderMatcher", "WithIncomingHeaderMatcher")
	})

	t.Run("invalid unescaping mode value", func(t *testing.T) {
		t.Parallel()
		got := recoverNewServeMuxPanic(t, WithUnescapingMode(UnescapingMode(99)))
		if got == nil {
			t.Fatal("invalid unescaping mode was accepted at construction")
		}
		if msg := got.(error).Error(); !strings.Contains(msg, "invalid value 99") {
			t.Fatalf("panic message %q does not point at the invalid value", msg)
		}
	})
}

func TestNewServeMux_RepeatedScalarWithSameValueIsIdempotent(t *testing.T) {
	t.Parallel()

	// Repeating a toggle, or a single-valued option with the identical value,
	// is harmless and keeps declaration order irrelevant.
	mux := NewServeMux(
		WithDisableChunkedEncoding(),
		WithDisableChunkedEncoding(),
		WithWriteContentLength(),
		WithUnescapingMode(UnescapingModeAllCharacters),
		WithUnescapingMode(UnescapingModeAllCharacters),
	)
	want := resolvedView{
		disableChunkedEncoding: true,
		writeContentLength:     true,
		unescapingMode:         UnescapingModeAllCharacters,
	}
	if got := viewOf(mux.cfg); got != want {
		t.Fatalf("resolved view = %+v; want %+v", got, want)
	}
}

func TestNewServeMux_DeclarationOrderDoesNotChangeSnapshot(t *testing.T) {
	t.Parallel()

	options := []ServeMuxOption{
		WithDisableChunkedEncoding(),
		WithWriteContentLength(),
		WithDisablePathLengthFallback(),
		WithDisableHTTPMethodOverride(),
		WithStrictDeepWildcardMatching(),
		WithUnescapingMode(UnescapingModeAllCharacters),
	}

	first := NewServeMux(options...)
	reverse := make([]ServeMuxOption, len(options))
	for i := range options {
		reverse[i] = options[len(options)-1-i]
	}
	second := NewServeMux(reverse...)

	if got, want := viewOf(first.cfg), viewOf(second.cfg); got != want {
		t.Fatalf("snapshot depends on declaration order:\nforward=%+v\nreverse=%+v", got, want)
	}
}

func TestNewServeMux_ListOptionsKeepAppendSemantics(t *testing.T) {
	t.Parallel()

	var order []string
	mk := func(name string) Middleware {
		return func(next HandlerFunc) HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
				order = append(order, name)
				next(w, r, pathParams)
			}
		}
	}

	mux := NewServeMux(WithMiddlewares(mk("a")), WithMiddlewares(mk("b"), mk("c")))
	if got, want := len(mux.middlewares), 3; got != want {
		t.Fatalf("len(middlewares) = %d; want %d (append semantics)", got, want)
	}
	if err := mux.HandlePath(http.MethodGet, "/foo", func(http.ResponseWriter, *http.Request, map[string]string) {}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/foo", nil)
	mux.ServeHTTP(httptest.NewRecorder(), req)
	if got, want := strings.Join(order, ","), "a,b,c"; got != want {
		t.Fatalf("middleware order = %q; want %q", got, want)
	}
}

func TestNewServeMux_FailedConstructionStopsAndLeavesNothing(t *testing.T) {
	t.Parallel()

	// An option placed after the conflicting declaration must not run, so the
	// failure cannot register even a single route.
	var laterOptionRan bool
	laterOption := ServeMuxOption(func(m *ServeMux) {
		laterOptionRan = true
		_ = m.HandlePath(http.MethodGet, "/later", func(http.ResponseWriter, *http.Request, map[string]string) {})
	})
	got := recoverNewServeMuxPanic(t,
		WithUnescapingMode(UnescapingModeAllCharacters),
		WithUnescapingMode(UnescapingModeLegacy),
		laterOption,
	)
	if got == nil {
		t.Fatal("conflicting construction did not panic")
	}
	if laterOptionRan {
		t.Fatal("an option after the conflicting declaration still ran; construction must stop at the conflict")
	}

	// A route registered BEFORE the conflict lives on the abandoned,
	// unreturned mux only: construction yields no ServeMux handle, so the
	// partial route set is unobservable.
	var earlyRegistrarRan bool
	earlyRegistrar := ServeMuxOption(func(m *ServeMux) {
		earlyRegistrarRan = true
		_ = m.HandlePath(http.MethodGet, "/early", func(http.ResponseWriter, *http.Request, map[string]string) {})
	})
	if got := recoverNewServeMuxPanic(t,
		earlyRegistrar,
		WithUnescapingMode(UnescapingModeAllCharacters),
		WithUnescapingMode(UnescapingModeLegacy),
	); got == nil {
		t.Fatal("conflicting construction did not panic")
	}
	if !earlyRegistrarRan {
		t.Fatal("test setup: the early registrar should have run before the conflict")
	}
}

func TestServeMux_SnapshotIsFrozenAfterConstruction(t *testing.T) {
	t.Parallel()

	mux := NewServeMux()

	// Applying an option after construction would mutate the snapshot shared
	// by in-flight requests, so it is rejected.
	var got any
	func() {
		defer func() { got = recover() }()
		WithDisableChunkedEncoding()(mux)
	}()
	if got == nil {
		t.Fatal("applying an option after NewServeMux did not panic; snapshot must be immutable")
	}

	// Defaults are frozen in.
	if mux.cfg.errorHandler == nil || mux.cfg.streamErrorHandler == nil ||
		mux.cfg.routingErrorHandler == nil || mux.cfg.incomingHeaderMatcher == nil ||
		mux.cfg.outgoingHeaderMatcher == nil || mux.cfg.outgoingTrailerMatcher == nil ||
		mux.cfg.forwardResponseRewriter == nil {
		t.Fatal("frozen snapshot is missing default hooks")
	}
	if !mux.frozen {
		t.Fatal("ServeMux.frozen should be set after NewServeMux")
	}
}

func TestMuxConfig_FramingPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		cfg              muxConfig
		streaming        bool
		forwardsTrailers bool
		wantChunk        bool
		wantLength       bool
	}{
		{"default buffered", muxConfig{}, false, false, false, false},
		{"default streaming", muxConfig{}, true, false, true, false},
		{"default buffered with trailers", muxConfig{}, false, true, true, false},
		{"default streaming with trailers", muxConfig{}, true, true, true, false},
		{"chunked off buffered", muxConfig{disableChunkedEncoding: true}, false, false, false, false},
		{"chunked off streaming", muxConfig{disableChunkedEncoding: true}, true, false, false, false},
		{"chunked off buffered with trailers", muxConfig{disableChunkedEncoding: true}, false, true, false, false},
		{"length on buffered", muxConfig{writeContentLength: true}, false, false, false, true},
		{"length on streaming", muxConfig{writeContentLength: true}, true, false, true, false},
		{"length on buffered with trailers", muxConfig{writeContentLength: true}, false, true, true, false},
		{"chunked off + length buffered", muxConfig{disableChunkedEncoding: true, writeContentLength: true}, false, false, false, true},
		{"chunked off + length streaming", muxConfig{disableChunkedEncoding: true, writeContentLength: true}, true, false, false, false},
		{"chunked off + length with trailers", muxConfig{disableChunkedEncoding: true, writeContentLength: true}, false, true, false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.cfg.writeChunkMarker(tc.streaming, tc.forwardsTrailers); got != tc.wantChunk {
				t.Errorf("writeChunkMarker(%v, %v) = %v; want %v", tc.streaming, tc.forwardsTrailers, got, tc.wantChunk)
			}
			if got := tc.cfg.writeContentLengthHeader(tc.streaming, tc.forwardsTrailers); got != tc.wantLength {
				t.Errorf("writeContentLengthHeader(%v, %v) = %v; want %v", tc.streaming, tc.forwardsTrailers, got, tc.wantLength)
			}
		})
	}
}

func TestMuxConfig_MethodOverrideAllowed(t *testing.T) {
	t.Parallel()

	formReq := func(method string, contentType string) *http.Request {
		r := httptest.NewRequest(method, "http://example.com/foo", strings.NewReader("a=b"))
		if contentType != "" {
			r.Header.Set("Content-Type", contentType)
		}
		return r
	}

	defaultCfg := muxConfig{}
	overrideOff := muxConfig{disableHTTPMethodOverride: true}
	// The fallback switch must have no say in method override availability.
	fallbackOff := muxConfig{disablePathLengthFallback: true}
	bothOff := muxConfig{disablePathLengthFallback: true, disableHTTPMethodOverride: true}

	form := formReq(http.MethodPost, "application/x-www-form-urlencoded")
	if !defaultCfg.methodOverrideAllowed(form) {
		t.Error("default: form POST should allow method override")
	}
	if !fallbackOff.methodOverrideAllowed(form) {
		t.Error("WithDisablePathLengthFallback must not disable method override")
	}
	if defaultCfg.methodOverrideAllowed(formReq(http.MethodGet, "application/x-www-form-urlencoded")) {
		t.Error("GET requests must not allow method override")
	}
	if defaultCfg.methodOverrideAllowed(formReq(http.MethodPost, "application/json")) {
		t.Error("non-form POST requests must not allow method override")
	}
	if !defaultCfg.methodOverrideAllowed(formReq(http.MethodPost, "application/x-www-form-urlencoded; charset=UTF-8")) {
		t.Error("form POST with charset parameter should allow method override")
	}
	if overrideOff.methodOverrideAllowed(form) {
		t.Error("WithDisableHTTPMethodOverride must disable method override")
	}
	if bothOff.methodOverrideAllowed(form) {
		t.Error("WithDisableHTTPMethodOverride must disable method override even with the fallback switch off")
	}
}
