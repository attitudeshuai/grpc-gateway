package runtime

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// DefaultMaxPathSegmentLength is the default maximum byte length of a single
// decoded path segment. It is used both for request path normalization and
// when validating path pattern literals in NewPattern.
const DefaultMaxPathSegmentLength = 4096

// DotSegmentAction determines how "." and ".." path segments are handled by a
// PathNormalizationRule.
type DotSegmentAction int

const (
	// DotSegmentNormalize resolves "." and ".." segments within the suffix of
	// the rule prefix using a bounded stack: "." is dropped and ".." pops the
	// previous suffix segment. A ".." that would traverse above the declared
	// rule prefix cannot be resolved and is rejected.
	DotSegmentNormalize DotSegmentAction = iota
	// DotSegmentReject rejects any request that contains a "." or ".."
	// segment.
	DotSegmentReject
)

// EmptySegmentAction determines how empty path segments caused by repeated
// path separators ("/v1//foo") are handled by a PathNormalizationRule.
type EmptySegmentAction int

const (
	// EmptySegmentCollapse drops empty segments that appear between other
	// segments. The trailing empty segment produced by a trailing slash is
	// governed by TrailingSlashAction instead.
	EmptySegmentCollapse EmptySegmentAction = iota
	// EmptySegmentReject rejects any request that contains an empty segment
	// between other segments.
	EmptySegmentReject
)

// TrailingSlashAction determines how the trailing empty segment produced by a
// trailing slash is handled by a PathNormalizationRule.
type TrailingSlashAction int

const (
	// TrailingSlashPreserve keeps the trailing empty segment, so trailing
	// slash and strict deep wildcard behavior stays identical to the mux's
	// behavior without normalization.
	TrailingSlashPreserve TrailingSlashAction = iota
	// TrailingSlashTrim removes the trailing empty segment before matching.
	// The single empty segment of the root path "/" is never removed.
	TrailingSlashTrim
)

// PathNormalizationRule declares how request paths under a registered literal
// prefix are decoded, validated and normalized before route matching.
//
// Rules are evaluated by longest matching prefix and only apply to requests
// whose path starts with Prefix. Requests not covered by any rule keep the
// exact historical behavior, byte for byte. The set of rules is compiled once
// in NewServeMux and cannot be changed afterwards; concurrent requests read
// the same immutable snapshot.
type PathNormalizationRule struct {
	// Prefix is a literal absolute path prefix such as "/v1" or "/v1/files".
	// It must start with "/", must not end with "/" (except for the root
	// prefix "/"), and must not contain empty segments, dot segments,
	// percent-encoding, wildcards or control bytes. The root prefix "/"
	// applies to every request.
	Prefix string

	// DotSegments selects the handling of "." and ".." segments. The zero
	// value DotSegmentNormalize provides bounded normalization.
	DotSegments DotSegmentAction

	// EmptySegments selects the handling of repeated path separators. The
	// zero value EmptySegmentCollapse folds internal empty segments.
	EmptySegments EmptySegmentAction

	// TrailingSlash selects the handling of a trailing slash. The zero
	// value TrailingSlashPreserve keeps historical behavior.
	TrailingSlash TrailingSlashAction

	// MaxSegmentLength is the maximum byte length of a decoded path
	// segment. A zero value uses DefaultMaxPathSegmentLength. The limit
	// applies to every decoded segment covered by the rule, including the
	// prefix segments; constructing a rule whose limit is shorter than one
	// of its own prefix segments panics.
	MaxSegmentLength int
}

// pathPolicy is the compiled, immutable representation of a
// PathNormalizationRule.
type pathPolicy struct {
	prefix     string
	prefixSegs []string
	dots       DotSegmentAction
	empties    EmptySegmentAction
	trailing   TrailingSlashAction
	maxSegLen  int
}

// compilePathPolicies validates the declared rules, compiles them into an
// immutable snapshot sorted from the longest prefix to the shortest, and
// panics on invalid configuration. NewServeMux has no error return, so
// invalid rules are programming errors that fail fast at construction.
func compilePathPolicies(rules ...PathNormalizationRule) []pathPolicy {
	if len(rules) == 0 {
		return nil
	}
	compiled := make([]pathPolicy, 0, len(rules))
	seen := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		if _, dup := seen[rule.Prefix]; dup {
			panic(fmt.Sprintf("runtime: duplicate path normalization rule for prefix %q", rule.Prefix))
		}
		switch rule.DotSegments {
		case DotSegmentNormalize, DotSegmentReject:
		default:
			panic(fmt.Sprintf("runtime: invalid DotSegments action %d for prefix %q", rule.DotSegments, rule.Prefix))
		}
		switch rule.EmptySegments {
		case EmptySegmentCollapse, EmptySegmentReject:
		default:
			panic(fmt.Sprintf("runtime: invalid EmptySegments action %d for prefix %q", rule.EmptySegments, rule.Prefix))
		}
		switch rule.TrailingSlash {
		case TrailingSlashPreserve, TrailingSlashTrim:
		default:
			panic(fmt.Sprintf("runtime: invalid TrailingSlash action %d for prefix %q", rule.TrailingSlash, rule.Prefix))
		}
		if rule.MaxSegmentLength < 0 {
			panic(fmt.Sprintf("runtime: negative MaxSegmentLength %d for prefix %q", rule.MaxSegmentLength, rule.Prefix))
		}
		maxSegLen := rule.MaxSegmentLength
		if maxSegLen == 0 {
			maxSegLen = DefaultMaxPathSegmentLength
		}
		prefixSegs := compilePolicyPrefix(rule.Prefix)
		for i, seg := range prefixSegs {
			if len(seg) > maxSegLen {
				panic(fmt.Sprintf("runtime: MaxSegmentLength %d is shorter than segment %d (%d bytes) of prefix %q",
					maxSegLen, i, len(seg), rule.Prefix))
			}
		}
		seen[rule.Prefix] = struct{}{}
		compiled = append(compiled, pathPolicy{
			prefix:     rule.Prefix,
			prefixSegs: prefixSegs,
			dots:       rule.DotSegments,
			empties:    rule.EmptySegments,
			trailing:   rule.TrailingSlash,
			maxSegLen:  maxSegLen,
		})
	}
	// Longest prefix first, with a lexicographic tie breaker for deterministic
	// selection. The snapshot is only read after construction.
	sort.Slice(compiled, func(i, j int) bool {
		if len(compiled[i].prefixSegs) != len(compiled[j].prefixSegs) {
			return len(compiled[i].prefixSegs) > len(compiled[j].prefixSegs)
		}
		return compiled[i].prefix < compiled[j].prefix
	})
	return compiled
}

// compilePolicyPrefix validates a rule prefix literal and returns its
// segments. It panics on any invalid declaration.
func compilePolicyPrefix(prefix string) []string {
	if !strings.HasPrefix(prefix, "/") {
		panic(fmt.Sprintf("runtime: path normalization prefix %q must start with \"/\"", prefix))
	}
	if prefix == "/" {
		return nil
	}
	if strings.HasSuffix(prefix, "/") {
		panic(fmt.Sprintf("runtime: path normalization prefix %q must not have a trailing slash", prefix))
	}
	segs := strings.Split(prefix[1:], "/")
	for i, seg := range segs {
		if seg == "" {
			panic(fmt.Sprintf("runtime: path normalization prefix %q has an empty segment at index %d", prefix, i))
		}
		if seg == "." || seg == ".." {
			panic(fmt.Sprintf("runtime: path normalization prefix %q contains a dot segment at index %d", prefix, i))
		}
		// Prefixes are literal: percent-encoding, template syntax, verbs and
		// ambiguous whitespace/query/fragment markers are refused.
		if strings.ContainsAny(seg, "%*{}?# :") {
			panic(fmt.Sprintf("runtime: path normalization prefix %q contains an unsupported character at segment %d", prefix, i))
		}
		if err := segmentContentError(seg, DefaultMaxPathSegmentLength); err != nil {
			panic(fmt.Sprintf("runtime: path normalization prefix %q has an invalid segment %d: %v", prefix, i, err))
		}
	}
	return segs
}

// selectPathPolicy returns the longest-prefix rule matching the raw (not yet
// normalized) path components, or nil when no rule applies. Prefix segments
// are compared literally and, to prevent percent-encoded prefixes from
// evading a rule in non-legacy unescaping modes, against the once-decoded
// segment value. The comparison decode is only used for selecting a rule; it
// does not mutate the components and is not visible in matching results.
func selectPathPolicy(policies []pathPolicy, components []string, mode UnescapingMode) *pathPolicy {
	for i := range policies {
		p := &policies[i]
		if len(components) < len(p.prefixSegs) {
			continue
		}
		matches := true
		for j, want := range p.prefixSegs {
			c := components[j]
			if c == want {
				continue
			}
			if mode == UnescapingModeLegacy {
				matches = false
				break
			}
			decoded, err := unescape(c, mode, true)
			if err != nil || decoded != want {
				matches = false
				break
			}
		}
		if matches {
			return p
		}
	}
	return nil
}

// normalizePathComponents performs the single decode-validate-normalize stage
// for request path components covered by a rule. It returns the components
// that must be used for both route matching and path parameter extraction.
// The input slice is never modified.
//
// Each component is decoded at most once: non-legacy modes decode per segment
// while honoring the configured UnescapingMode, and legacy components (already
// decoded once by net/url) are taken as-is. Malformed percent escapes surface
// the existing MalformedSequenceError.
func normalizePathComponents(components []string, mode UnescapingMode, p *pathPolicy) ([]string, error) {
	k := len(p.prefixSegs)
	if len(components) < k {
		// Defensive: selectPathPolicy guarantees this cannot happen.
		return nil, fmt.Errorf("invalid path: shorter than the registered prefix %q", p.prefix)
	}

	decoded := make([]string, len(components))
	for i, seg := range components {
		if mode == UnescapingModeLegacy {
			// net/url has already decoded URL.Path once; a literal '%' that
			// resulted from decoding "%25" is indistinguishable from a
			// malformed escape here, so escapes are not re-validated in
			// legacy mode. Content, length and dot checks still run on the
			// decoded value.
			decoded[i] = seg
			continue
		}
		d, err := unescape(seg, mode, true)
		if err != nil {
			return nil, err
		}
		decoded[i] = d
	}

	for _, seg := range decoded {
		if err := segmentContentError(seg, p.maxSegLen); err != nil {
			return nil, fmt.Errorf("invalid path: %w", err)
		}
	}

	trailingSlash := decoded[len(decoded)-1] == ""

	// Prefix segments are fixed and can never be popped by "..".
	out := make([]string, k, len(decoded))
	copy(out, decoded[:k])
	suffixDepth := 0
	for i := k; i < len(decoded); i++ {
		seg := decoded[i]
		if seg == "" {
			if i == len(decoded)-1 {
				// Trailing empty segment: handled after the loop.
				continue
			}
			if p.empties == EmptySegmentReject {
				return nil, fmt.Errorf("invalid path: empty segment from a repeated path separator at index %d is not allowed", i)
			}
			continue
		}
		// The mux strips a ":verb" suffix from the last component, and it
		// does so per pattern after normalization. A component such as
		// "..:custom" or "..:a:custom" (verbs may contain colons) would turn
		// into the dot segment ".." once the verb is stripped, and a verb
		// pattern's wildcard could then carry that dot segment upstream.
		// Account for the dot segment hiding behind any colon prefix of the
		// last component. The component is dropped when resolved: a bare verb
		// cannot bind to a pattern. An encoded colon ("%3a") is not a verb
		// separator in any unescaping mode, so such a component stays opaque.
		dot := seg
		if i == len(decoded)-1 {
			// See the comment above: a dot segment glued to a verb suffix is
			// resolved as the dot segment, and the whole last component is
			// dropped (a bare verb cannot bind to a pattern).
			if base := dotSegmentBeforeVerb(seg); base != "" {
				dot = base
			}
		}
		if dot == "." || dot == ".." {
			if p.dots == DotSegmentReject {
				return nil, fmt.Errorf("invalid path: dot segment %q at index %d is not allowed", dot, i)
			}
			if dot == ".." {
				if suffixDepth == 0 {
					return nil, fmt.Errorf("invalid path: %q at index %d traverses beyond the registered prefix %q", dot, i, p.prefix)
				}
				out = out[:len(out)-1]
				suffixDepth--
			}
			continue
		}
		out = append(out, seg)
		suffixDepth++
	}

	if trailingSlash && (p.trailing == TrailingSlashPreserve || len(out) == 0) {
		out = append(out, "")
	} else if len(out) == 0 {
		// A path whose dot segments resolve to the root (e.g. root policy
		// and "/a/..") still needs the root component for matching.
		out = append(out, "")
	}
	return out, nil
}

// dotSegmentBeforeVerb returns "." or ".." when seg has a colon at a
// position where stripping a ":verb" suffix would leave exactly that dot
// segment, e.g. "..:custom" or "..:a:custom" (verbs may contain colons). It
// returns "" for ordinary segments and for a bare verb such as ":custom",
// whose colon is at index 0.
func dotSegmentBeforeVerb(seg string) string {
	for i := 1; i < len(seg); i++ {
		if seg[i] != ':' {
			continue
		}
		switch seg[:i] {
		case ".", "..":
			return seg[:i]
		}
	}
	return ""
}

// segmentContentError validates the content of a decoded path segment and
// returns a stable, deterministic error describing the first violation. The
// same checks apply to request segments and to pattern literals. The empty
// segment is valid: emptiness is handled by the normalization rules.
func segmentContentError(seg string, maxLen int) error {
	if len(seg) > maxLen {
		return fmt.Errorf("segment length %d exceeds the limit of %d bytes", len(seg), maxLen)
	}
	for i := 0; i < len(seg); i++ {
		if c := seg[i]; c < 0x20 || c == 0x7f {
			return fmt.Errorf("control character in segment %s", quotePathSegment(seg))
		}
	}
	if !utf8.ValidString(seg) {
		return fmt.Errorf("invalid UTF-8 byte in segment %s", quotePathSegment(seg))
	}
	return nil
}

// quotePathSegment quotes a segment for inclusion in error messages,
// truncating long segments to keep the message bounded.
func quotePathSegment(seg string) string {
	const max = 64
	if len(seg) <= max {
		return strconv.Quote(seg)
	}
	// Cut at a rune boundary so the quoted output stays valid UTF-8.
	cut := max
	for cut > 0 && !utf8.RuneStart(seg[cut]) {
		cut--
	}
	return strconv.Quote(seg[:cut]) + "..."
}

// validatePatternLiteral applies the same structural checks used for request
// path segments to a literal segment while a Pattern is constructed. It runs
// at route loading time for both generated patterns (via MustPattern) and
// custom patterns (via HandlePath), so an invalid template fails before the
// route table is modified. The empty literal representing the root template
// "/" is allowed. OpCapture operands such as field paths ("id.value") are not
// template segments and are not validated here.
func validatePatternLiteral(lit string, segment int) error {
	if lit == "." || lit == ".." {
		return fmt.Errorf("%w: literal %q at segment %d is a dot segment", ErrInvalidPattern, lit, segment)
	}
	if lit == "" {
		return nil
	}
	if err := segmentContentError(lit, DefaultMaxPathSegmentLength); err != nil {
		return fmt.Errorf("%w: invalid literal at segment %d: %v", ErrInvalidPattern, segment, err)
	}
	return nil
}
