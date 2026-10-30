package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/grpc-ecosystem/grpc-gateway/v2/utilities"
)

// recorder collects handler invocations so rejection tests can prove that no
// handler (and therefore no upstream call) ran.
type recorder struct {
	mu        sync.Mutex
	calls     int
	lastPath  string
	lastQuery string
	lastBody  string
}

func (r *recorder) record(params map[string]string, query, body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	for _, v := range params {
		r.lastPath = v
	}
	r.lastQuery = query
	r.lastBody = body
}

func (r *recorder) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// litPattern builds a pattern of consecutive literal segments.
func litPattern(lits ...string) ([]int, []string) {
	ops := make([]int, 0, 2*len(lits))
	for i := range lits {
		ops = append(ops, int(utilities.OpLitPush), i)
	}
	return ops, lits
}

// deepPattern builds /lits.../{var=**}.
func deepPattern(varName string, lits ...string) ([]int, []string) {
	ops, pool := litPattern(lits...)
	ops = append(ops, int(utilities.OpPushM), 0, int(utilities.OpConcatN), 1)
	pool = append(pool, varName)
	ops = append(ops, int(utilities.OpCapture), len(pool)-1)
	return ops, pool
}

// wildcardPattern builds /lits.../{var}.
func wildcardPattern(varName string, lits ...string) ([]int, []string) {
	ops, pool := litPattern(lits...)
	ops = append(ops, int(utilities.OpPush), 0, int(utilities.OpConcatN), 1)
	pool = append(pool, varName)
	ops = append(ops, int(utilities.OpCapture), len(pool)-1)
	return ops, pool
}

func mustPattern(t *testing.T, ops []int, pool []string, verb string, opts ...runtime.PatternOpt) runtime.Pattern {
	t.Helper()
	pat, err := runtime.NewPatternWithOptions(1, ops, pool, verb, opts...)
	if err != nil {
		t.Fatalf("runtime.NewPatternWithOptions(%v, %v, %q) failed: %v", ops, pool, verb, err)
	}
	return pat
}

// normalizationTestMux holds a mux with the routes used across these tests.
type normalizationTestMux struct {
	mux *runtime.ServeMux
	rec *recorder
}

func newNormalizationTestMux(t *testing.T, opts ...runtime.ServeMuxOption) normalizationTestMux {
	t.Helper()
	rec := &recorder{}
	allOpts := append([]runtime.ServeMuxOption{}, opts...)
	mux := runtime.NewServeMux(allOpts...)

	register := func(method string, pat runtime.Pattern) {
		p := pat
		mux.Handle(method, p, func(w http.ResponseWriter, r *http.Request, params map[string]string) {
			body, _ := io.ReadAll(r.Body)
			var v string
			for _, value := range params {
				v = value
			}
			rec.record(params, r.URL.RawQuery, string(body))
			fmt.Fprintf(w, "%s %s %s", method, p.String(), v)
		})
	}

	registerDeep := func(method, varName string, lits ...string) {
		ops, pool := deepPattern(varName, lits...)
		register(method, mustPattern(t, ops, pool, ""))
	}
	registerLit := func(method, verb string, lits ...string) {
		ops, pool := litPattern(lits...)
		register(method, mustPattern(t, ops, pool, verb))
	}
	// Registered first so deeper routes (prepended on Handle) sort ahead of
	// it; otherwise the catch-all would shadow /v1/files/** and /v1/echo/**.
	ops, pool := wildcardPattern("name", "v1")
	register(http.MethodGet, mustPattern(t, ops, pool, ""))
	registerDeep(http.MethodGet, "path", "v1", "files")
	registerDeep(http.MethodGet, "x", "v1", "echo")
	registerDeep(http.MethodPost, "x", "v1", "echo")
	registerLit(http.MethodGet, "custom", "v1", "short")
	// Registered last so the plain literal sorts ahead of every wildcard.
	registerLit(http.MethodGet, "", "v1", "short")
	return normalizationTestMux{mux: mux, rec: rec}
}

// newFilesOnlyMux builds a mux exposing only GET /v1/files/{path=**}, used
// where a catch-all /v1/{name} route would otherwise mask the 404 or strict
// deep wildcard behavior under test.
func newFilesOnlyMux(t *testing.T, strict bool, rules ...runtime.PathNormalizationRule) normalizationTestMux {
	t.Helper()
	opts := []runtime.ServeMuxOption{runtime.WithPathNormalization(rules...)}
	if strict {
		opts = append(opts, runtime.WithStrictDeepWildcardMatching())
	}
	mt := normalizationTestMux{mux: runtime.NewServeMux(opts...), rec: &recorder{}}
	dops, dpool := deepPattern("path", "v1", "files")
	pat := mustPattern(t, dops, dpool, "", strictPatternOpt(strict)...)
	mt.mux.Handle(http.MethodGet, pat, func(_ http.ResponseWriter, r *http.Request, params map[string]string) {
		mt.rec.record(params, r.URL.RawQuery, "")
	})
	return mt
}

func strictPatternOpt(strict bool) []runtime.PatternOpt {
	if strict {
		return []runtime.PatternOpt{runtime.WithStrictDeepWildcard()}
	}
	return nil
}

func serve(mux *runtime.ServeMux, method, target string, rawPath string, headers map[string]string, body string) *httptest.ResponseRecorder {
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	r := httptest.NewRequest(method, target, rdr)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	if rawPath != "" {
		r.URL.RawPath = rawPath
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

// assertRoutingErrorShape asserts the existing error body shape produced for
// malformed path input: HTTP 400 carrying a gRPC Status body
// ({"code":2,"message":"...","details":[]}) through the error handler.
func assertRoutingErrorShape(t *testing.T, w *httptest.ResponseRecorder, wantStatus int) string {
	t.Helper()
	if w.Code != wantStatus {
		t.Fatalf("status = %d; want %d (body %q)", w.Code, wantStatus, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body %q is not JSON: %v", w.Body.String(), err)
	}
	if _, ok := body["code"]; !ok {
		t.Errorf("body %q has no code field", w.Body.String())
	}
	msg, ok := body["message"].(string)
	if !ok || msg == "" {
		t.Errorf("body %q has no non-empty message", w.Body.String())
	}
	details, ok := body["details"].([]any)
	if !ok || len(details) != 0 {
		t.Errorf("body %q details = %v; want empty array", w.Body.String(), body["details"])
	}
	return msg
}

func defaultV1Rules() []runtime.PathNormalizationRule {
	return []runtime.PathNormalizationRule{{Prefix: "/v1"}}
}

func TestServeMux_PathNormalization_DotsResolveAndHit(t *testing.T) {
	mt := newNormalizationTestMux(t, runtime.WithPathNormalization(defaultV1Rules()...))

	for _, tc := range []struct {
		name string
		path string
		want string
	}{
		{"single dot", "/v1/files/./a", "a"},
		{"double dot pops", "/v1/files/a/../b", "b"},
		{"dot before subtree", "/v1/a/../files/x", "x"},
		{"dot chain", "/v1/files/./a/../c", "c"},
		{"dot in echo subtree", "/v1/echo/x/./y", "x/y"},
		{"trailing single dot", "/v1/files/a/.", "a"},
		{"double dot to prefix", "/v1/files/a/..", ""},
		{"encoded dot legacy", "/v1/files/%2e/a", "a"},
		{"encoded double dot legacy", "/v1/files/a/%2e%2e/b", "b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := serve(mt.mux, http.MethodGet, "https://example.com"+tc.path, "", nil, "")
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d; want 200 (body %q)", w.Code, w.Body.String())
			}
			if mt.rec.lastPath != tc.want {
				t.Errorf("captured path = %q; want %q (body %q)", mt.rec.lastPath, tc.want, w.Body.String())
			}
		})
	}

	// Non-legacy unescaping modes decode percent-encoded dots before
	// normalization, so they cannot smuggle dot segments either.
	for _, mode := range []runtime.UnescapingMode{
		runtime.UnescapingModeAllExceptReserved,
		runtime.UnescapingModeAllExceptSlash,
		runtime.UnescapingModeAllCharacters,
	} {
		mt := newNormalizationTestMux(t,
			runtime.WithUnescapingMode(mode),
			runtime.WithPathNormalization(defaultV1Rules()...),
		)
		w := serve(mt.mux, http.MethodGet, "https://example.com/v1/files/%2e/a", "/v1/files/%2e/a", nil, "")
		if w.Code != http.StatusOK || mt.rec.lastPath != "a" {
			t.Errorf("mode %d: code=%d captured=%q body=%q; want 200/a", mode, w.Code, mt.rec.lastPath, w.Body.String())
		}
		w = serve(mt.mux, http.MethodGet, "https://example.com/v1/files/a/%2e%2e/b", "/v1/files/a/%2e%2e/b", nil, "")
		if w.Code != http.StatusOK || mt.rec.lastPath != "b" {
			t.Errorf("mode %d: code=%d captured=%q body=%q; want 200/b", mode, w.Code, mt.rec.lastPath, w.Body.String())
		}
	}

	// A verb suffix is still stripped after dot normalization.
	w := serve(mt.mux, http.MethodGet, "https://example.com/v1/x/../short:custom", "", nil, "")
	if w.Code != http.StatusOK {
		t.Errorf("verb after normalization: status = %d body = %q; want 200", w.Code, w.Body.String())
	}
}

func TestServeMux_PathNormalization_BoundaryReject(t *testing.T) {
	// Rules on both the prefix and the registered subtree.
	subtree := newNormalizationTestMux(t, runtime.WithPathNormalization(
		runtime.PathNormalizationRule{Prefix: "/v1"},
		runtime.PathNormalizationRule{Prefix: "/v1/files"},
	))
	for _, path := range []string{
		"/v1/files/../secret",
		"/v1/../etc",
		"/v1/a/../../b",
	} {
		w := serve(subtree.mux, http.MethodGet, "https://example.com"+path, "", nil, "")
		assertRoutingErrorShape(t, w, http.StatusBadRequest)
		if subtree.rec.callCount() != 0 {
			t.Errorf("%s: handler ran %d times; want 0", path, subtree.rec.callCount())
		}
	}

	// A prefix-only rule normalizes within /v1: with only the
	// /v1/files/{path=**} route registered, /v1/files/../secret becomes
	// /v1/secret, which has no route -> deterministic 404, still never handed
	// to the wildcard with raw dots.
	prefixOnly := newFilesOnlyMux(t, false, defaultV1Rules()...)
	w := serve(prefixOnly.mux, http.MethodGet, "https://example.com/v1/files/../secret", "", nil, "")
	if w.Code != http.StatusNotFound {
		t.Errorf("prefix-only /v1/files/../secret: status = %d body = %q; want 404", w.Code, w.Body.String())
	}
	if prefixOnly.rec.callCount() != 0 {
		t.Errorf("handler ran %d times; want 0", prefixOnly.rec.callCount())
	}

	// Explicit reject policy refuses every dot segment.
	reject := newNormalizationTestMux(t, runtime.WithPathNormalization(
		runtime.PathNormalizationRule{Prefix: "/v1", DotSegments: runtime.DotSegmentReject},
	))
	w = serve(reject.mux, http.MethodGet, "https://example.com/v1/files/./a", "", nil, "")
	assertRoutingErrorShape(t, w, http.StatusBadRequest)
	w = serve(reject.mux, http.MethodGet, "https://example.com/v1/files/a/b", "", nil, "")
	if w.Code != http.StatusOK {
		t.Errorf("plain request under reject policy: status = %d body = %q; want 200", w.Code, w.Body.String())
	}
	if reject.rec.callCount() != 1 {
		t.Errorf("reject mux calls = %d; want 1", reject.rec.callCount())
	}

	// Encoded dot pairs are rejected in legacy (net/url decoded Path) and in
	// non-legacy modes (RawPath decoded once during normalization).
	cases := []struct {
		path    string
		rawPath string
		mode    runtime.UnescapingMode
	}{
		{"/v1/%2e%2e/etc", "", runtime.UnescapingModeDefault},
		{"/v1/files/%2e%2e/x", "/v1/files/%2e%2e/x", runtime.UnescapingModeAllExceptReserved},
		{"/v1/files/%2E%2E/x", "/v1/files/%2E%2E/x", runtime.UnescapingModeAllExceptSlash},
	}
	for i, tc := range cases {
		opts := []runtime.ServeMuxOption{runtime.WithPathNormalization(
			runtime.PathNormalizationRule{Prefix: "/v1/files"},
			runtime.PathNormalizationRule{Prefix: "/v1"},
		)}
		if tc.mode != runtime.UnescapingModeDefault {
			opts = append(opts, runtime.WithUnescapingMode(tc.mode))
		}
		mt := newNormalizationTestMux(t, opts...)
		w := serve(mt.mux, http.MethodGet, "https://example.com"+tc.path, tc.rawPath, nil, "")
		if w.Code != http.StatusBadRequest {
			t.Errorf("case %d mode %d: status = %d body = %q; want 400", i, tc.mode, w.Code, w.Body.String())
		}
	}
}

func TestServeMux_PathNormalization_EmptyAndTrailing(t *testing.T) {
	echoCollapse := newNormalizationTestMux(t,
		runtime.WithPathNormalization(runtime.PathNormalizationRule{Prefix: "/v1"}))
	echoReject := newNormalizationTestMux(t,
		runtime.WithPathNormalization(runtime.PathNormalizationRule{
			Prefix: "/v1", EmptySegments: runtime.EmptySegmentReject,
		}))
	echoTrim := newNormalizationTestMux(t,
		runtime.WithPathNormalization(runtime.PathNormalizationRule{
			Prefix: "/v1", TrailingSlash: runtime.TrailingSlashTrim,
		}))

	// Strict deep wildcard semantics need a mux without a catch-all
	// /v1/{name} route masking /v1/files.
	filesPreserve := newFilesOnlyMux(t, true, runtime.PathNormalizationRule{Prefix: "/v1"})
	filesReject := newFilesOnlyMux(t, true, runtime.PathNormalizationRule{
		Prefix: "/v1", EmptySegments: runtime.EmptySegmentReject,
	})
	filesTrim := newFilesOnlyMux(t, true, runtime.PathNormalizationRule{
		Prefix: "/v1", TrailingSlash: runtime.TrailingSlashTrim,
	})

	for _, tc := range []struct {
		name       string
		mt         normalizationTestMux
		path       string
		wantStatus int
		wantPath   string
	}{
		{"collapse internal", echoCollapse, "/v1/echo//a", http.StatusOK, "a"},
		{"collapse keeps trailing empty", echoCollapse, "/v1/echo/a//", http.StatusOK, "a/"},
		{"single trailing preserved", echoCollapse, "/v1/echo/a/", http.StatusOK, "a/"},
		{"reject internal", echoReject, "/v1/echo//a", http.StatusBadRequest, ""},
		{"reject keeps trailing", echoReject, "/v1/echo/a/", http.StatusOK, "a/"},
		{"trim internal collapsed", echoTrim, "/v1/echo//a", http.StatusOK, "a"},
		{"trim trailing removed", echoTrim, "/v1/echo/a/", http.StatusOK, "a"},

		// Strict deep wildcard interactions must remain stable.
		{"strict files trailing slash preserved", filesPreserve, "/v1/files/", http.StatusOK, ""},
		{"strict files no slash 404", filesPreserve, "/v1/files", http.StatusNotFound, ""},
		{"reject files no slash 404", filesReject, "/v1/files", http.StatusNotFound, ""},
		{"reject files trailing slash preserved", filesReject, "/v1/files/", http.StatusOK, ""},
		{"trim files trailing slash removed", filesTrim, "/v1/files/", http.StatusNotFound, ""},
		{"trim files no slash 404", filesTrim, "/v1/files", http.StatusNotFound, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := serve(tc.mt.mux, http.MethodGet, "https://example.com"+tc.path, "", nil, "")
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d body = %q; want %d", w.Code, w.Body.String(), tc.wantStatus)
			}
			if tc.wantStatus == http.StatusOK && tc.mt.rec.lastPath != tc.wantPath {
				t.Errorf("captured = %q; want %q (body %q)", tc.mt.rec.lastPath, tc.wantPath, w.Body.String())
			}
		})
	}
}

func TestServeMux_PathNormalization_InvalidContent(t *testing.T) {
	mt := newNormalizationTestMux(t, runtime.WithPathNormalization(defaultV1Rules()...))
	for _, tc := range []struct {
		name   string
		path   string
		substr string
	}{
		{"nul", "/v1/%00", "control character"},
		{"control after text", "/v1/a%01b", "control character"},
		{"del", "/v1/%7f", "control character"},
		{"invalid utf8", "/v1/%ff", "invalid UTF-8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := serve(mt.mux, http.MethodGet, "https://example.com"+tc.path, "", nil, "")
			msg := assertRoutingErrorShape(t, w, http.StatusBadRequest)
			if !bytes.Contains([]byte(msg), []byte(tc.substr)) {
				t.Errorf("message %q does not contain %q", msg, tc.substr)
			}
			if mt.rec.callCount() != 0 {
				t.Errorf("handler ran; want 0 calls")
			}
		})
	}

	// Custom segment length limit.
	limited := newNormalizationTestMux(t, runtime.WithPathNormalization(
		runtime.PathNormalizationRule{Prefix: "/v1", MaxSegmentLength: 8},
	))
	w := serve(limited.mux, http.MethodGet, "https://example.com/v1/abcdefghi", "", nil, "")
	assertRoutingErrorShape(t, w, http.StatusBadRequest)
	w = serve(limited.mux, http.MethodGet, "https://example.com/v1/abcdefgh", "", nil, "")
	if w.Code != http.StatusOK {
		t.Errorf("boundary-length segment: status = %d body = %q; want 200", w.Code, w.Body.String())
	}

	// Malformed escapes in non-legacy modes keep the existing
	// MalformedSequenceError wording.
	for _, tc := range []struct {
		path    string
		rawPath string
		substr  string
	}{
		{"/v1/zz", "/v1/%zz", `malformed path escape "%zz"`},
		{"/v1/short", "/v1/%", `malformed path escape "%"`},
	} {
		nonLegacy := newNormalizationTestMux(t,
			runtime.WithUnescapingMode(runtime.UnescapingModeAllExceptReserved),
			runtime.WithPathNormalization(defaultV1Rules()...),
		)
		w := serve(nonLegacy.mux, http.MethodGet, "https://example.com"+tc.path, tc.rawPath, nil, "")
		msg := assertRoutingErrorShape(t, w, http.StatusBadRequest)
		if msg != tc.substr {
			t.Errorf("message = %q; want %q", msg, tc.substr)
		}
	}
}

func TestServeMux_PathNormalization_ParityWithPlainMux(t *testing.T) {
	plain := newNormalizationTestMux(t, runtime.WithStrictDeepWildcardMatching())
	normalized := newNormalizationTestMux(t,
		runtime.WithStrictDeepWildcardMatching(),
		runtime.WithPathNormalization(defaultV1Rules()...),
	)
	for _, tc := range []struct {
		name    string
		method  string
		path    string
		headers map[string]string
	}{
		{"literal priority", http.MethodGet, "/v1/short", nil},
		{"wildcard capture", http.MethodGet, "/v1/name-123", nil},
		{"verb suffix", http.MethodGet, "/v1/short:custom", nil},
		{"colon component captured", http.MethodGet, "/v1/:custom", nil},
		{"strict deep wildcard slash", http.MethodGet, "/v1/files/", nil},
		{"strict deep wildcard no slash", http.MethodGet, "/v1/files", nil},
		{"unmatched", http.MethodGet, "/v2/nope", nil},
		{"post form get fallback", http.MethodPost, "/v1/short",
			map[string]string{"Content-Type": "application/x-www-form-urlencoded"}},
		{"method not allowed", http.MethodDelete, "/v1/short", nil},
		{"json post wrong method", http.MethodPost, "/v1/short",
			map[string]string{"Content-Type": "application/json"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w1 := serve(plain.mux, tc.method, "https://example.com"+tc.path, "", tc.headers, "")
			w2 := serve(normalized.mux, tc.method, "https://example.com"+tc.path, "", tc.headers, "")
			if w1.Code != w2.Code || w1.Body.String() != w2.Body.String() {
				t.Errorf("plain=%d/%q normalized=%d/%q; want identical results",
					w1.Code, w1.Body.String(), w2.Code, w2.Body.String())
			}
		})
	}
}

func TestServeMux_PathNormalization_NoPolicyKeepsLegacyBehavior(t *testing.T) {
	// Without any declared rule, dot segments and repeated separators keep
	// being captured verbatim by wildcards, exactly as before the feature.
	mt := newNormalizationTestMux(t)
	w := serve(mt.mux, http.MethodGet, "https://example.com/v1/files/../x", "", nil, "")
	if w.Code != http.StatusOK || mt.rec.lastPath != "../x" {
		t.Errorf("dot legacy: code=%d captured=%q body=%q; want 200/../x",
			w.Code, mt.rec.lastPath, w.Body.String())
	}
	w = serve(mt.mux, http.MethodGet, "https://example.com/v1/echo//a", "", nil, "")
	if w.Code != http.StatusOK || mt.rec.lastPath != "/a" {
		t.Errorf("empty legacy: code=%d captured=%q body=%q; want 200//a",
			w.Code, mt.rec.lastPath, w.Body.String())
	}
}

func TestServeMux_PathNormalization_CustomErrorHandlers(t *testing.T) {
	var routingStatuses []int
	var errorHandlerCalls int
	var muxErr error
	mux := runtime.NewServeMux(
		runtime.WithPathNormalization(defaultV1Rules()...),
		runtime.WithErrorHandler(func(_ context.Context, _ *runtime.ServeMux, _ runtime.Marshaler,
			w http.ResponseWriter, _ *http.Request, err error,
		) {
			errorHandlerCalls++
			muxErr = err
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "custom-error")
		}),
		runtime.WithRoutingErrorHandler(func(_ context.Context, _ *runtime.ServeMux, _ runtime.Marshaler,
			w http.ResponseWriter, _ *http.Request, status int,
		) {
			routingStatuses = append(routingStatuses, status)
			w.WriteHeader(status)
		}),
	)
	hits := 0
	litOps, litPool := litPattern("v1", "short")
	mux.Handle(http.MethodGet, mustPattern(t, litOps, litPool, ""),
		func(http.ResponseWriter, *http.Request, map[string]string) { hits++ })

	w := serve(mux, http.MethodGet, "https://example.com/v1/../x", "", nil, "")
	if w.Code != http.StatusBadRequest || w.Body.String() != "custom-error" {
		t.Errorf("custom error handler not used: %d %q", w.Code, w.Body.String())
	}
	var statusErr *runtime.HTTPStatusError
	if !errors.As(muxErr, &statusErr) || statusErr.HTTPStatus != http.StatusBadRequest {
		t.Errorf("error handler received %v; want HTTPStatusError 400", muxErr)
	}

	serve(mux, http.MethodGet, "https://example.com/v1/missing", "", nil, "")
	serve(mux, http.MethodDelete, "https://example.com/v1/short", "", nil, "")
	if len(routingStatuses) != 2 || routingStatuses[0] != http.StatusNotFound ||
		routingStatuses[1] != http.StatusMethodNotAllowed {
		t.Errorf("routing statuses = %v; want [404 405]", routingStatuses)
	}
	if errorHandlerCalls != 1 || hits != 0 {
		t.Errorf("errorHandlerCalls=%d hits=%d; want 1/0", errorHandlerCalls, hits)
	}
}

func TestServeMux_PathNormalization_QueryAndBodyUntouched(t *testing.T) {
	mt := newNormalizationTestMux(t, runtime.WithPathNormalization(defaultV1Rules()...))
	w := serve(mt.mux, http.MethodPost,
		"https://example.com/v1/echo/./a?z=1&n=2", "",
		map[string]string{"Content-Type": "application/json"}, "hello-body")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %q; want 200", w.Code, w.Body.String())
	}
	if mt.rec.lastQuery != "z=1&n=2" {
		t.Errorf("query = %q; want z=1&n=2", mt.rec.lastQuery)
	}
	if mt.rec.lastBody != "hello-body" {
		t.Errorf("body = %q; want hello-body", mt.rec.lastBody)
	}
	if mt.rec.lastPath != "a" {
		t.Errorf("captured = %q; want a", mt.rec.lastPath)
	}
}

func TestServeMux_PathNormalization_Concurrent(t *testing.T) {
	mt := newNormalizationTestMux(t, runtime.WithPathNormalization(defaultV1Rules()...))
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				switch (seed + i) % 3 {
				case 0:
					if w := serve(mt.mux, http.MethodGet, "https://example.com/v1/files/a/../b", "", nil, ""); w.Code != http.StatusOK {
						t.Errorf("normalized request: %d", w.Code)
					}
				case 1:
					if w := serve(mt.mux, http.MethodGet, "https://example.com/v1/../x", "", nil, ""); w.Code != http.StatusBadRequest {
						t.Errorf("boundary request: %d", w.Code)
					}
				default:
					if w := serve(mt.mux, http.MethodGet, "https://example.com/other/../x", "", nil, ""); w.Code != http.StatusNotFound {
						t.Errorf("unprefixed request: %d", w.Code)
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestServeMux_HandlePath_DotLiteralRejected(t *testing.T) {
	mux := runtime.NewServeMux()
	called := false
	h := func(http.ResponseWriter, *http.Request, map[string]string) { called = true }
	if err := mux.HandlePath(http.MethodGet, "/v1/../x", h); err == nil {
		t.Fatal("HandlePath with /v1/../x succeeded; want error")
	}
	w := serve(mux, http.MethodGet, "https://example.com/v1/../x", "", nil, "")
	if w.Code != http.StatusNotFound || called {
		t.Errorf("failed HandlePath left a route behind: code=%d called=%v", w.Code, called)
	}
	if err := mux.HandlePath(http.MethodGet, "/v1/ok", h); err != nil {
		t.Fatalf("HandlePath /v1/ok failed: %v", err)
	}
	w = serve(mux, http.MethodGet, "https://example.com/v1/ok", "", nil, "")
	if w.Code != http.StatusOK {
		t.Errorf("valid HandlePath route: code=%d; want 200", w.Code)
	}
}

// TestServeMux_PathNormalization_UnescapingModeCaptures pins capture values
// when a normalization rule is combined with a non-default UnescapingMode:
// multi-segment (deep wildcard) captures stay byte-for-byte identical to the
// historical pipeline, while single-segment captures become mode-faithful
// (reserved characters and "%2F" stay encoded).
func TestServeMux_PathNormalization_UnescapingModeCaptures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       runtime.UnescapingMode
		path       string
		rawPath    string
		wantSingle string
		wantDeep   string
	}{
		{"reserved single", runtime.UnescapingModeAllExceptReserved, "/v1/%21", "/v1/%21", "%21", ""},
		{"slash single", runtime.UnescapingModeAllExceptSlash, "/v1/a%2Fb", "/v1/a%2Fb", "a%2Fb", ""},
		{"reserved deep", runtime.UnescapingModeAllExceptReserved, "/v1/files/a%21b", "/v1/files/a%21b", "", "a%21b"},
		{"slash deep", runtime.UnescapingModeAllExceptSlash, "/v1/files/x%2Fy", "/v1/files/x%2Fy", "", "x%2Fy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plain := newNormalizationTestMux(t, runtime.WithUnescapingMode(tc.mode))
			normalized := newNormalizationTestMux(t,
				runtime.WithUnescapingMode(tc.mode),
				runtime.WithPathNormalization(defaultV1Rules()...),
			)
			if tc.wantSingle != "" {
				w1 := serve(plain.mux, http.MethodGet, "https://example.com"+tc.path, tc.rawPath, nil, "")
				w2 := serve(normalized.mux, http.MethodGet, "https://example.com"+tc.path, tc.rawPath, nil, "")
				if w1.Code != http.StatusOK || w2.Code != http.StatusOK {
					t.Fatalf("status plain=%d normalized=%d", w1.Code, w2.Code)
				}
				if normalized.rec.lastPath != tc.wantSingle {
					t.Errorf("single capture = %q; want mode-faithful %q", normalized.rec.lastPath, tc.wantSingle)
				}
			}
			if tc.wantDeep != "" {
				w1 := serve(plain.mux, http.MethodGet, "https://example.com"+tc.path, tc.rawPath, nil, "")
				w2 := serve(normalized.mux, http.MethodGet, "https://example.com"+tc.path, tc.rawPath, nil, "")
				if w1.Code != http.StatusOK || w2.Code != http.StatusOK {
					t.Fatalf("status plain=%d normalized=%d", w1.Code, w2.Code)
				}
				if plain.rec.lastPath != tc.wantDeep || normalized.rec.lastPath != tc.wantDeep {
					t.Errorf("deep capture plain=%q normalized=%q; want %q on both",
						plain.rec.lastPath, normalized.rec.lastPath, tc.wantDeep)
				}
			}
		})
	}
}

// newVerbGlueMux registers GET /v1/files/{path=**}:custom and
// GET /v1/{name}:custom, the routes on which a dot segment glued to a verb
// suffix used to reach the wildcard capture after the verb was stripped.
func newVerbGlueMux(t *testing.T, rules ...runtime.PathNormalizationRule) normalizationTestMux {
	t.Helper()
	mt := normalizationTestMux{mux: runtime.NewServeMux(runtime.WithPathNormalization(rules...)), rec: &recorder{}}
	dops, dpool := deepPattern("path", "v1", "files")
	mt.mux.Handle(http.MethodGet, mustPattern(t, dops, dpool, "custom"),
		func(_ http.ResponseWriter, r *http.Request, params map[string]string) {
			mt.rec.record(params, r.URL.RawQuery, "")
		})
	sops, spool := wildcardPattern("name", "v1")
	mt.mux.Handle(http.MethodGet, mustPattern(t, sops, spool, "custom"),
		func(_ http.ResponseWriter, r *http.Request, params map[string]string) {
			mt.rec.record(params, r.URL.RawQuery, "")
		})
	return mt
}

func TestServeMux_PathNormalization_DotGluedToVerb(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rules      []runtime.PathNormalizationRule
		path       string
		wantStatus int
	}{
		// Boundary of the /v1/files subtree: the glued ".." cannot pop above it.
		{"subtree normalize boundary", []runtime.PathNormalizationRule{{Prefix: "/v1/files"}}, "/v1/files/..:custom", http.StatusBadRequest},
		// In-bound resolutions drop the glued component: no verb pattern can
		// bind after it is gone, and a wildcard never sees a raw dot.
		{"subtree normalize single dot", []runtime.PathNormalizationRule{{Prefix: "/v1/files"}}, "/v1/files/.:custom", http.StatusNotFound},
		{"subtree normalize pop", []runtime.PathNormalizationRule{{Prefix: "/v1/files"}}, "/v1/files/a/..:custom", http.StatusNotFound},
		{"subtree reject", []runtime.PathNormalizationRule{{Prefix: "/v1/files", DotSegments: runtime.DotSegmentReject}}, "/v1/files/..:custom", http.StatusBadRequest},
		{"subtree reject single dot", []runtime.PathNormalizationRule{{Prefix: "/v1/files", DotSegments: runtime.DotSegmentReject}}, "/v1/files/.:custom", http.StatusBadRequest},
		// Under the /v1 prefix the pop removes the "files" component; no
		// pattern matches /v1:custom, and the wildcard never sees a raw dot.
		{"prefix normalize pops in bounds", []runtime.PathNormalizationRule{{Prefix: "/v1"}}, "/v1/files/..:custom", http.StatusNotFound},
		{"prefix single wildcard boundary", []runtime.PathNormalizationRule{{Prefix: "/v1"}}, "/v1/..:custom", http.StatusBadRequest},
		// Verbs containing colons are stripped at any registered verb length.
		{"colon verb glue", []runtime.PathNormalizationRule{{Prefix: "/v1/files"}}, "/v1/files/..:a:custom", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mt := newVerbGlueMux(t, tc.rules...)
			w := serve(mt.mux, http.MethodGet, "https://example.com"+tc.path, "", nil, "")
			if w.Code != tc.wantStatus {
				t.Errorf("status = %d body = %q; want %d", w.Code, w.Body.String(), tc.wantStatus)
			}
			if tc.wantStatus == http.StatusBadRequest && mt.rec.callCount() != 0 {
				t.Errorf("handler invoked %d times; want 0 on rejection", mt.rec.callCount())
			}
			if w.Code == http.StatusOK {
				for _, seg := range strings.Split(mt.rec.lastPath, "/") {
					if seg == "." || seg == ".." {
						t.Errorf("captured %q contains raw dot segment %q", mt.rec.lastPath, seg)
					}
				}
			}
		})
	}

	// Sanity: legitimate verb requests still match and capture correctly.
	mt := newVerbGlueMux(t, runtime.PathNormalizationRule{Prefix: "/v1"})
	w := serve(mt.mux, http.MethodGet, "https://example.com/v1/files/real:custom", "", nil, "")
	if w.Code != http.StatusOK || mt.rec.lastPath != "real" {
		t.Errorf("deep verb route: code=%d captured=%q; want 200/real", w.Code, mt.rec.lastPath)
	}
	w = serve(mt.mux, http.MethodGet, "https://example.com/v1/thing:custom", "", nil, "")
	if w.Code != http.StatusOK || mt.rec.lastPath != "thing" {
		t.Errorf("single verb route: code=%d captured=%q; want 200/thing", w.Code, mt.rec.lastPath)
	}
}
