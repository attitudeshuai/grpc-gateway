package runtime

import (
	"errors"
	"strings"
	"testing"
)

func mustPolicies(t *testing.T, rules ...PathNormalizationRule) []pathPolicy {
	t.Helper()
	return compilePathPolicies(rules...)
}

func defaultRule(prefix string) PathNormalizationRule {
	return PathNormalizationRule{Prefix: prefix}
}

func TestCompilePathPolicies_Valid(t *testing.T) {
	policies := mustPolicies(t,
		defaultRule("/v1"),
		defaultRule("/v1/files"),
		defaultRule("/"),
		PathNormalizationRule{Prefix: "/v2", MaxSegmentLength: 128},
	)
	if got, want := len(policies), 4; got != want {
		t.Fatalf("len(policies) = %d; want %d", got, want)
	}
	// Sorted longest first: /v1/files (2), /v1 (1), /v2 (1), / (0).
	gotPrefixes := []string{
		policies[0].prefix,
		policies[1].prefix,
		policies[2].prefix,
		policies[3].prefix,
	}
	wantPrefixes := []string{"/v1/files", "/v1", "/v2", "/"}
	for i := range wantPrefixes {
		if gotPrefixes[i] != wantPrefixes[i] {
			t.Errorf("policies[%d].prefix = %q; want %q (full order: %v)", i, gotPrefixes[i], wantPrefixes[i], gotPrefixes)
		}
	}
	if policies[3].maxSegLen != DefaultMaxPathSegmentLength {
		t.Errorf("default maxSegLen = %d; want %d", policies[3].maxSegLen, DefaultMaxPathSegmentLength)
	}
	if policies[0].dots != DotSegmentNormalize || policies[0].empties != EmptySegmentCollapse || policies[0].trailing != TrailingSlashPreserve {
		t.Errorf("zero-value actions are not the documented defaults: %+v", policies[0])
	}
}

func TestCompilePathPolicies_InvalidPanics(t *testing.T) {
	invalid := []PathNormalizationRule{
		{Prefix: "v1"},       // no leading slash
		{Prefix: "/v1/"},     // trailing slash
		{Prefix: "/v1//x"},   // empty segment
		{Prefix: "/v1/../x"}, // dot segment
		{Prefix: "/v1/./x"},  // dot segment
		{Prefix: "/v1/%31"},  // percent encoding
		{Prefix: "/v1/*"},    // wildcard
		{Prefix: "/v1/{x}"},  // template syntax
		{Prefix: "/v1/ "},    // space
		{Prefix: "/v1/a:b"},  // verb separator
		{Prefix: "/v1/\x00"}, // control character
		{Prefix: "/v1", DotSegments: 99},
		{Prefix: "/v1", EmptySegments: 99},
		{Prefix: "/v1", TrailingSlash: 99},
		{Prefix: "/v1", MaxSegmentLength: -1},
		{Prefix: "/v1long", MaxSegmentLength: 1}, // shorter than a prefix segment
	}
	for i, rule := range invalid {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("#%d %+v: compilePathPolicies succeeded; want panic", i, rule)
				}
			}()
			compilePathPolicies(rule)
		}()
	}
	// Duplicate prefixes panic.
	func() {
		defer func() {
			if recover() == nil {
				t.Errorf("duplicate prefixes: compilePathPolicies succeeded; want panic")
			}
		}()
		compilePathPolicies(defaultRule("/v1"), defaultRule("/v1"))
	}()

	// A limit equal to the longest prefix segment length is accepted.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("limit equal to segment length panicked: %v", r)
			}
		}()
		compilePathPolicies(PathNormalizationRule{Prefix: "/ab", MaxSegmentLength: 2})
	}()
}

func TestSelectPathPolicy(t *testing.T) {
	policies := mustPolicies(t,
		defaultRule("/v1/files"),
		defaultRule("/v1"),
		defaultRule("/other"),
	)
	for _, tc := range []struct {
		name       string
		components []string
		mode       UnescapingMode
		want       string
	}{
		{"exact nested", []string{"v1", "files", "x"}, UnescapingModeLegacy, "/v1/files"},
		{"longest wins", []string{"v1", "x"}, UnescapingModeLegacy, "/v1"},
		{"other prefix", []string{"other", "x"}, UnescapingModeLegacy, "/other"},
		{"no match", []string{"v2", "x"}, UnescapingModeLegacy, ""},
		{"prefix itself", []string{"v1"}, UnescapingModeLegacy, "/v1"},
		{"trailing slash", []string{"v1", ""}, UnescapingModeLegacy, "/v1"},
		{"encoded prefix cannot evade", []string{"%76%31", "x"}, UnescapingModeAllExceptReserved, "/v1"},
		{"encoded uppercase prefix", []string{"%76%31", "files", "a"}, UnescapingModeAllCharacters, "/v1/files"},
		{"encoded prefix legacy stays decoded", []string{"v1", "x"}, UnescapingModeDefault, "/v1"},
		{"malformed encoded prefix does not match", []string{"%zz", "x"}, UnescapingModeAllExceptReserved, ""},
		{"encoded slash cannot fuse prefix", []string{"v1%2Ffiles", "x"}, UnescapingModeAllExceptSlash, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := selectPathPolicy(policies, tc.components, tc.mode)
			if tc.want == "" {
				if got != nil {
					t.Errorf("selectPathPolicy() = %q; want nil", got.prefix)
				}
				return
			}
			if got == nil {
				t.Fatalf("selectPathPolicy() = nil; want %q", tc.want)
			}
			if got.prefix != tc.want {
				t.Errorf("selectPathPolicy() = %q; want %q", got.prefix, tc.want)
			}
		})
	}

	// A root policy matches everything when declared alone.
	root := mustPolicies(t, defaultRule("/"))
	if got := selectPathPolicy(root, []string{"anything", "..", "x"}, UnescapingModeLegacy); got == nil || got.prefix != "/" {
		t.Errorf("root policy did not match all paths: %#v", got)
	}
}

func TestNormalizePathComponents_Dots(t *testing.T) {
	p := &mustPolicies(t, defaultRule("/v1"))[0]
	for _, tc := range []struct {
		name string
		raw  []string
		want []string
	}{
		{"plain", []string{"v1", "a"}, []string{"v1", "a"}},
		{"single dot drops", []string{"v1", "files", ".", "a"}, []string{"v1", "files", "a"}},
		{"double dot pops", []string{"v1", "files", "a", "..", "b"}, []string{"v1", "files", "b"}},
		{"dot before prefix content", []string{"v1", "a", "..", "files", "x"}, []string{"v1", "files", "x"}},
		{"double dot at suffix root resolves to prefix", []string{"v1", "a", ".."}, []string{"v1"}},
		{"single dot at end", []string{"v1", "a", "."}, []string{"v1", "a"}},
		{"double dot pops", []string{"v1", "a", "b", "..", "..", "c"}, []string{"v1", "c"}},
		{"literal-looking segment pops within prefix", []string{"v1", "files", "..", "secret"}, []string{"v1", "secret"}},
		{"two separate dot segments both drop", []string{"v1", ".", ".", "etc"}, []string{"v1", "etc"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := append([]string(nil), tc.raw...)
			got, err := normalizePathComponents(tc.raw, UnescapingModeLegacy, p)
			if err != nil {
				t.Fatalf("normalizePathComponents(%v) failed: %v", tc.raw, err)
			}
			if !equalStrings(got, tc.want) {
				t.Errorf("normalizePathComponents(%v) = %v; want %v", tc.raw, got, tc.want)
			}
			if !equalStrings(tc.raw, original) {
				t.Errorf("input slice was modified: %v", tc.raw)
			}
		})
	}
}

func TestNormalizePathComponents_DotsRejected(t *testing.T) {
	p := &mustPolicies(t, PathNormalizationRule{Prefix: "/v1", DotSegments: DotSegmentReject})[0]
	for _, raw := range [][]string{
		{"v1", "."},
		{"v1", ".."},
		{"v1", "files", "%2e%2e", "x"},
		{"v1", "files", "%2e", "%2e", "x"},
	} {
		got, err := normalizePathComponents(raw, UnescapingModeAllExceptReserved, p)
		if err == nil {
			t.Errorf("normalizePathComponents(%v) = %v; want error", raw, got)
		}
	}
}

func TestNormalizePathComponents_BoundaryReject(t *testing.T) {
	v1 := &mustPolicies(t, defaultRule("/v1"))[0]
	files := &mustPolicies(t, defaultRule("/v1/files"))[0]
	for _, tc := range []struct {
		name string
		p    *pathPolicy
		raw  []string
		mode UnescapingMode
	}{
		{"above prefix", v1, []string{"v1", "..", "etc"}, UnescapingModeLegacy},
		{"above prefix after pop", v1, []string{"v1", "a", "..", "..", "b"}, UnescapingModeLegacy},
		{"double escape", v1, []string{"v1", "x", "..", "..", "v1", "a"}, UnescapingModeLegacy},
		{"registered subtree root", files, []string{"v1", "files", "..", "secret"}, UnescapingModeLegacy},
		{"encoded dots legacy", v1, []string{"v1", "..", "etc"}, UnescapingModeLegacy},
		{"encoded dots reserved mode", v1, []string{"v1", "%2e%2e", "etc"}, UnescapingModeAllExceptReserved},
		{"encoded uppercase dots", v1, []string{"v1", "%2E%2E", "etc"}, UnescapingModeAllExceptSlash},
		{"encoded dots all characters", v1, []string{"v1", "%2e%2e", "etc"}, UnescapingModeAllCharacters},
		{"encoded dots at subtree root", files, []string{"v1", "files", "%2e%2e", "x"}, UnescapingModeAllExceptReserved},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizePathComponents(tc.raw, tc.mode, tc.p)
			if err == nil {
				t.Errorf("normalizePathComponents(%v) = %v; want boundary error", tc.raw, got)
				return
			}
			if !strings.Contains(err.Error(), `prefix "`+tc.p.prefix+`"`) {
				t.Errorf("error %q does not name the declared prefix %q", err.Error(), tc.p.prefix)
			}
		})
	}

	// Root policy: a leading ".." cannot be resolved within the root.
	root := &mustPolicies(t, defaultRule("/"))[0]
	if _, err := normalizePathComponents([]string{"..", "etc"}, UnescapingModeLegacy, root); err == nil {
		t.Errorf("root policy normalized %q above the root; want error", "/../etc")
	}
}

func TestNormalizePathComponents_EmptyAndTrailing(t *testing.T) {
	collapse := &mustPolicies(t, PathNormalizationRule{Prefix: "/v1"})[0]
	reject := &mustPolicies(t, PathNormalizationRule{Prefix: "/v1", EmptySegments: EmptySegmentReject})[0]
	trim := &mustPolicies(t, PathNormalizationRule{Prefix: "/v1", TrailingSlash: TrailingSlashTrim})[0]

	for _, tc := range []struct {
		name string
		p    *pathPolicy
		raw  []string
		want []string
		err  bool
	}{
		{"collapse internal", collapse, []string{"v1", "", "a"}, []string{"v1", "a"}, false},
		{"collapse multiple", collapse, []string{"v1", "", "", "a", ""}, []string{"v1", "a", ""}, false},
		{"preserve trailing", collapse, []string{"v1", "a", ""}, []string{"v1", "a", ""}, false},
		{"reject internal", reject, []string{"v1", "", "a"}, nil, true},
		{"reject keeps trailing allowed", reject, []string{"v1", "a", ""}, []string{"v1", "a", ""}, false},
		{"trim trailing", trim, []string{"v1", "a", ""}, []string{"v1", "a"}, false},
		{"trim internal still collapses", trim, []string{"v1", "", "a", ""}, []string{"v1", "a"}, false},
		{"trim keeps prefix root", trim, []string{"v1", ""}, []string{"v1"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizePathComponents(tc.raw, UnescapingModeLegacy, tc.p)
			if tc.err {
				if err == nil {
					t.Errorf("got %v; want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !equalStrings(got, tc.want) {
				t.Errorf("got %v; want %v", got, tc.want)
			}
		})
	}

	// The root path keeps its single empty component for every trailing mode.
	for _, action := range []TrailingSlashAction{TrailingSlashPreserve, TrailingSlashTrim} {
		root := &mustPolicies(t, PathNormalizationRule{Prefix: "/", TrailingSlash: action})[0]
		got, err := normalizePathComponents([]string{""}, UnescapingModeLegacy, root)
		if err != nil {
			t.Fatalf("root path action %d: %v", action, err)
		}
		if !equalStrings(got, []string{""}) {
			t.Errorf("root path action %d = %v; want [\"\"]", action, got)
		}
	}
}

func TestNormalizePathComponents_ContentValidation(t *testing.T) {
	p := &mustPolicies(t, PathNormalizationRule{Prefix: "/v1", MaxSegmentLength: 4})[0]
	for _, tc := range []struct {
		name      string
		raw       []string
		mode      UnescapingMode
		errSubstr string
	}{
		{"control nul", []string{"v1", "a\x00b"}, UnescapingModeLegacy, "control character"},
		{"control unit separator", []string{"v1", "a\x1fb"}, UnescapingModeLegacy, "control character"},
		{"del", []string{"v1", "a\x7fb"}, UnescapingModeLegacy, "control character"},
		{"invalid utf8", []string{"v1", "a\xffb"}, UnescapingModeLegacy, "invalid UTF-8"},
		{"too long", []string{"v1", "abcde"}, UnescapingModeLegacy, "exceeds the limit"},
		{"encoded nul", []string{"v1", "%00"}, UnescapingModeAllExceptReserved, "control character"},
		{"encoded del", []string{"v1", "%7F"}, UnescapingModeAllCharacters, "control character"},
		{"encoded invalid utf8", []string{"v1", "%ff"}, UnescapingModeAllExceptSlash, "invalid UTF-8"},
		{"malformed escape", []string{"v1", "%zz"}, UnescapingModeAllExceptReserved, "malformed path escape"},
		{"truncated escape", []string{"v1", "%"}, UnescapingModeAllExceptReserved, "malformed path escape"},
		{"valid utf8 iri", []string{"v1", "%C3%A9"}, UnescapingModeAllExceptReserved, ""},
		{"at limit boundary", []string{"v1", "abcd"}, UnescapingModeLegacy, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizePathComponents(tc.raw, tc.mode, p)
			if tc.errSubstr == "" {
				if err != nil {
					t.Errorf("unexpected error: %v (got %v)", err, got)
				}
				return
			}
			if err == nil {
				t.Fatalf("got %v; want error containing %q", got, tc.errSubstr)
			}
			if !strings.Contains(err.Error(), tc.errSubstr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.errSubstr)
			}
		})
	}
}

func TestNormalizePathComponents_DecodeModes(t *testing.T) {
	p := &mustPolicies(t, defaultRule("/v1"))[0]
	for _, tc := range []struct {
		name string
		mode UnescapingMode
		raw  []string
		want []string
	}{
		{
			"reserved stays encoded",
			UnescapingModeAllExceptReserved,
			[]string{"v1", "%21"},
			[]string{"v1", "%21"},
		},
		{
			"unreserved decodes",
			UnescapingModeAllExceptReserved,
			[]string{"v1", "%61%62"},
			[]string{"v1", "ab"},
		},
		{
			"slash stays encoded in except-slash mode",
			UnescapingModeAllExceptSlash,
			[]string{"v1", "a%2Fb"},
			[]string{"v1", "a%2Fb"},
		},
		{
			"all characters decodes",
			UnescapingModeAllCharacters,
			[]string{"v1", "ab"},
			[]string{"v1", "ab"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizePathComponents(tc.raw, tc.mode, p)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !equalStrings(got, tc.want) {
				t.Errorf("got %v; want %v", got, tc.want)
			}
		})
	}
}

func TestNormalizePathComponents_LegacyLiteralPercent(t *testing.T) {
	// In legacy mode net/url has already decoded URL.Path once, including
	// "%25" into a literal '%'. That value must not be mistaken for a
	// malformed escape.
	p := &mustPolicies(t, defaultRule("/v1"))[0]
	got, err := normalizePathComponents([]string{"v1", "50%", "a"}, UnescapingModeLegacy, p)
	if err != nil {
		t.Fatalf("literal percent rejected in legacy mode: %v", err)
	}
	if !equalStrings(got, []string{"v1", "50%", "a"}) {
		t.Errorf("got %v; want literal percent preserved", got)
	}
}

func TestSegmentContentError(t *testing.T) {
	long := strings.Repeat("a", DefaultMaxPathSegmentLength+1)
	for _, tc := range []struct {
		name      string
		seg       string
		errSubstr string
	}{
		{"empty ok", "", ""},
		{"plain ok", "a.b- c?", ""}, // space 0x20 is not a control character
		{"long", long, "exceeds the limit"},
		{"nul", "a\x00", "control character"},
		{"ff", "\xff", "invalid UTF-8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := segmentContentError(tc.seg, DefaultMaxPathSegmentLength)
			if tc.errSubstr == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
				t.Errorf("segmentContentError(%q) = %v; want substring %q", tc.seg, err, tc.errSubstr)
			}
		})
	}

	// quotePathSegment truncates long content without producing invalid UTF-8.
	q := quotePathSegment(strings.Repeat("é", 100))
	if !strings.HasSuffix(q, "...") {
		t.Errorf("quotePathSegment truncation suffix missing: %q", q)
	}
}

func TestNormalizePathComponents_Deterministic(t *testing.T) {
	p := &mustPolicies(t, defaultRule("/v1"))[0]
	raw := []string{"v1", "a", ".", "b", "..", "c", ""}
	first, err := normalizePathComponents(raw, UnescapingModeLegacy, p)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	for i := 0; i < 10; i++ {
		got, err := normalizePathComponents(raw, UnescapingModeLegacy, p)
		if err != nil || !equalStrings(got, first) {
			t.Fatalf("call %d = %v, %v; want %v, nil", i, got, err, first)
		}
	}
	// Malformed inputs fail with stable messages.
	bad := &mustPolicies(t, defaultRule("/v1"))[0]
	_, err1 := normalizePathComponents([]string{"v1", "..", "x"}, UnescapingModeLegacy, bad)
	_, err2 := normalizePathComponents([]string{"v1", "..", "x"}, UnescapingModeLegacy, bad)
	if err1 == nil || err1.Error() != err2.Error() {
		t.Fatalf("errors not stable: %v vs %v", err1, err2)
	}
	var mse MalformedSequenceError
	_, malformed := normalizePathComponents([]string{"v1", "%0"}, UnescapingModeAllExceptReserved, bad)
	if !errors.As(malformed, &mse) {
		t.Errorf("malformed escape error = %v; want MalformedSequenceError", malformed)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestDotSegmentBeforeVerb(t *testing.T) {
	for _, tc := range []struct {
		seg  string
		want string
	}{
		{"..:custom", ".."},
		{".:custom", "."},
		{"..:a:custom", ".."},
		{"a..:custom", ""},
		{"..x", ""},
		{":custom", ""},
		{"short:custom", ""},
		{"a:b:c", ""},
		{"..", ""},
		{".", ""},
		{"%2e%2e:custom", ""}, // the encoded colon spelling is not a verb
	} {
		if got := dotSegmentBeforeVerb(tc.seg); got != tc.want {
			t.Errorf("dotSegmentBeforeVerb(%q) = %q; want %q", tc.seg, got, tc.want)
		}
	}
}

func TestNormalizePathComponents_DotGluedToVerb(t *testing.T) {
	// A ":verb" suffix is stripped per pattern only after normalization, so
	// the last component must account for a dot segment glued in front of a
	// verb; otherwise the dot would reach a verb pattern's wildcard raw.
	filesNorm := &mustPolicies(t, defaultRule("/v1/files"))[0]
	filesReject := &mustPolicies(t, PathNormalizationRule{Prefix: "/v1/files", DotSegments: DotSegmentReject})[0]
	v1Norm := &mustPolicies(t, defaultRule("/v1"))[0]

	for _, tc := range []struct {
		name    string
		p       *pathPolicy
		raw     []string
		want    []string
		wantErr bool
	}{
		{"boundary under subtree", filesNorm, []string{"v1", "files", "..:custom"}, nil, true},
		{"single dot glue drops", filesNorm, []string{"v1", "files", ".:custom"}, []string{"v1", "files"}, false},
		{"pop then glue drops", filesNorm, []string{"v1", "files", "a", "..:custom"}, []string{"v1", "files"}, false},
		{"colon verb containing colons", filesNorm, []string{"v1", "files", "..:a:custom"}, nil, true},
		{"reject double dot glue", filesReject, []string{"v1", "files", "..:custom"}, nil, true},
		{"reject single dot glue", filesReject, []string{"v1", "files", ".:custom"}, nil, true},
		{"pop within v1 prefix", v1Norm, []string{"v1", "files", "..:custom"}, []string{"v1"}, false},
		{"plain verb component unchanged", v1Norm, []string{"v1", "short:custom"}, []string{"v1", "short:custom"}, false},
		{"bare verb unchanged", v1Norm, []string{"v1", ":custom"}, []string{"v1", ":custom"}, false},
		{"non-last glue is opaque", v1Norm, []string{"v1", "..:x", "y"}, []string{"v1", "..:x", "y"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizePathComponents(tc.raw, UnescapingModeLegacy, tc.p)
			if tc.wantErr {
				if err == nil {
					t.Errorf("got %v; want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !equalStrings(got, tc.want) {
				t.Errorf("got %v; want %v", got, tc.want)
			}
		})
	}
}
