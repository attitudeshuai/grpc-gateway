package runtime

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/utilities"
)

const (
	validVersion = 1
	anything     = 0
)

func TestNewPattern(t *testing.T) {
	for _, spec := range []struct {
		ops  []int
		pool []string
		verb string

		stackSizeWant, tailLenWant int
	}{
		{},
		{
			ops:           []int{int(utilities.OpNop), anything},
			stackSizeWant: 0,
			tailLenWant:   0,
		},
		{
			ops:           []int{int(utilities.OpPush), anything},
			stackSizeWant: 1,
			tailLenWant:   0,
		},
		{
			ops:           []int{int(utilities.OpLitPush), 0},
			pool:          []string{"abc"},
			stackSizeWant: 1,
			tailLenWant:   0,
		},
		{
			ops:           []int{int(utilities.OpPushM), anything},
			stackSizeWant: 1,
			tailLenWant:   0,
		},
		{
			ops: []int{
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 1,
			},
			stackSizeWant: 1,
			tailLenWant:   0,
		},
		{
			ops: []int{
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 0,
			},
			pool:          []string{"abc"},
			stackSizeWant: 1,
			tailLenWant:   0,
		},
		{
			ops: []int{
				int(utilities.OpPush), anything,
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPushM), anything,
				int(utilities.OpConcatN), 2,
				int(utilities.OpCapture), 2,
			},
			pool:          []string{"lit1", "lit2", "var1"},
			stackSizeWant: 4,
			tailLenWant:   0,
		},
		{
			ops: []int{
				int(utilities.OpPushM), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 2,
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
			},
			pool:          []string{"lit1", "lit2", "var1"},
			stackSizeWant: 2,
			tailLenWant:   2,
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPushM), anything,
				int(utilities.OpLitPush), 2,
				int(utilities.OpConcatN), 3,
				int(utilities.OpLitPush), 3,
				int(utilities.OpCapture), 4,
			},
			pool:          []string{"lit1", "lit2", "lit3", "lit4", "var1"},
			stackSizeWant: 4,
			tailLenWant:   2,
		},
		{
			ops:           []int{int(utilities.OpLitPush), 0},
			pool:          []string{"abc"},
			verb:          "LOCK",
			stackSizeWant: 1,
			tailLenWant:   0,
		},
	} {
		pat, err := NewPattern(validVersion, spec.ops, spec.pool, spec.verb)
		if err != nil {
			t.Errorf("NewPattern(%d, %v, %q, %q) failed with %v; want success", validVersion, spec.ops, spec.pool, spec.verb, err)
			continue
		}
		if got, want := pat.stacksize, spec.stackSizeWant; got != want {
			t.Errorf("pat.stacksize = %d; want %d", got, want)
		}
		if got, want := pat.tailLen, spec.tailLenWant; got != want {
			t.Errorf("pat.stacksize = %d; want %d", got, want)
		}
	}
}

func TestNewPatternWithWrongOp(t *testing.T) {
	for _, spec := range []struct {
		ops  []int
		pool []string
		verb string
	}{
		{
			// op code out of bound
			ops: []int{-1, anything},
		},
		{
			// op code out of bound
			ops: []int{int(utilities.OpEnd), 0},
		},
		{
			// odd number of items
			ops: []int{int(utilities.OpPush)},
		},
		{
			// negative index
			ops:  []int{int(utilities.OpLitPush), -1},
			pool: []string{"abc"},
		},
		{
			// index out of bound
			ops:  []int{int(utilities.OpLitPush), 1},
			pool: []string{"abc"},
		},
		{
			// negative # of segments
			ops:  []int{int(utilities.OpConcatN), -1},
			pool: []string{"abc"},
		},
		{
			// negative index
			ops:  []int{int(utilities.OpCapture), -1},
			pool: []string{"abc"},
		},
		{
			// index out of bound
			ops:  []int{int(utilities.OpCapture), 1},
			pool: []string{"abc"},
		},
		{
			// pushM appears twice
			ops: []int{
				int(utilities.OpPushM), anything,
				int(utilities.OpLitPush), 0,
				int(utilities.OpPushM), anything,
			},
			pool: []string{"abc"},
		},
	} {
		_, err := NewPattern(validVersion, spec.ops, spec.pool, spec.verb)
		if err == nil {
			t.Errorf("NewPattern(%d, %v, %q, %q) succeeded; want failure with %v", validVersion, spec.ops, spec.pool, spec.verb, ErrInvalidPattern)
			continue
		}
		if !errors.Is(err, ErrInvalidPattern) {
			t.Errorf("NewPattern(%d, %v, %q, %q) failed with %v; want failure with %v", validVersion, spec.ops, spec.pool, spec.verb, err, ErrInvalidPattern)
			continue
		}
	}
}

func TestNewPatternWithStackUnderflow(t *testing.T) {
	for _, spec := range []struct {
		ops  []int
		pool []string
		verb string
	}{
		{
			ops: []int{int(utilities.OpConcatN), 1},
		},
		{
			ops:  []int{int(utilities.OpCapture), 0},
			pool: []string{"abc"},
		},
	} {
		_, err := NewPattern(validVersion, spec.ops, spec.pool, spec.verb)
		if err == nil {
			t.Errorf("NewPattern(%d, %v, %q, %q) succeeded; want failure with %v", validVersion, spec.ops, spec.pool, spec.verb, ErrInvalidPattern)
			continue
		}
		if !errors.Is(err, ErrInvalidPattern) {
			t.Errorf("NewPattern(%d, %v, %q, %q) failed with %v; want failure with %v", validVersion, spec.ops, spec.pool, spec.verb, err, ErrInvalidPattern)
			continue
		}
	}
}

func TestMatch(t *testing.T) {
	for _, spec := range []struct {
		ops  []int
		pool []string
		verb string

		match    []string
		notMatch []string
	}{
		{
			match:    []string{""},
			notMatch: []string{"example"},
		},
		{
			ops:      []int{int(utilities.OpNop), anything},
			match:    []string{""},
			notMatch: []string{"example", "path/to/example"},
		},
		{
			ops:      []int{int(utilities.OpPush), anything},
			match:    []string{"abc", "def"},
			notMatch: []string{"", "abc/def"},
		},
		{
			ops:      []int{int(utilities.OpLitPush), 0},
			pool:     []string{"v1"},
			match:    []string{"v1"},
			notMatch: []string{"", "v2"},
		},
		{
			ops:   []int{int(utilities.OpPushM), anything},
			match: []string{"", "abc", "abc/def", "abc/def/ghi"},
		},
		{
			ops: []int{
				int(utilities.OpPushM), anything,
				int(utilities.OpLitPush), 0,
			},
			pool:  []string{"tail"},
			match: []string{"tail", "abc/tail", "abc/def/tail"},
			notMatch: []string{
				"", "abc", "abc/def",
				"tail/extra", "abc/tail/extra", "abc/def/tail/extra",
			},
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 2,
			},
			pool:  []string{"v1", "bucket", "name"},
			match: []string{"v1/bucket/my-bucket", "v1/bucket/our-bucket"},
			notMatch: []string{
				"",
				"v1",
				"v1/bucket",
				"v2/bucket/my-bucket",
				"v1/pubsub/my-topic",
			},
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPushM), anything,
				int(utilities.OpConcatN), 2,
				int(utilities.OpCapture), 2,
			},
			pool: []string{"v1", "o", "name"},
			match: []string{
				"v1/o",
				"v1/o/my-bucket",
				"v1/o/our-bucket",
				"v1/o/my-bucket/dir",
				"v1/o/my-bucket/dir/dir2",
				"v1/o/my-bucket/dir/dir2/obj",
			},
			notMatch: []string{
				"",
				"v1",
				"v2/o/my-bucket",
				"v1/b/my-bucket",
			},
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 2,
				int(utilities.OpCapture), 2,
				int(utilities.OpLitPush), 3,
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 4,
			},
			pool: []string{"v2", "b", "name", "o", "oname"},
			match: []string{
				"v2/b/my-bucket/o/obj",
				"v2/b/our-bucket/o/obj",
				"v2/b/my-bucket/o/dir",
			},
			notMatch: []string{
				"",
				"v2",
				"v2/b",
				"v2/b/my-bucket",
				"v2/b/my-bucket/o",
			},
		},
		{
			ops:      []int{int(utilities.OpLitPush), 0},
			pool:     []string{"v1"},
			verb:     "LOCK",
			match:    []string{"v1:LOCK"},
			notMatch: []string{"v1", "LOCK"},
		},
	} {
		pat, err := NewPattern(validVersion, spec.ops, spec.pool, spec.verb)
		if err != nil {
			t.Errorf("NewPattern(%d, %v, %q, %q) failed with %v; want success", validVersion, spec.ops, spec.pool, spec.verb, err)
			continue
		}

		for _, path := range spec.match {
			_, err = pat.Match(segments(path))
			if err != nil {
				t.Errorf("pat.Match(%q) failed with %v; want success; pattern = (%v, %q)", path, err, spec.ops, spec.pool)
			}
		}

		for _, path := range spec.notMatch {
			_, err = pat.Match(segments(path))
			if err == nil {
				t.Errorf("pat.Match(%q) succeeded; want failure with %v; pattern = (%v, %q)", path, ErrNotMatch, spec.ops, spec.pool)
				continue
			}
			if !errors.Is(err, ErrNotMatch) {
				t.Errorf("pat.Match(%q) failed with %v; want failure with %v; pattern = (%v, %q)", spec.notMatch, err, ErrNotMatch, spec.ops, spec.pool)
			}
		}
	}
}

func TestMatchWithBinding(t *testing.T) {
	for _, spec := range []struct {
		ops  []int
		pool []string
		path string
		verb string
		mode UnescapingMode

		want map[string]string
	}{
		{
			want: make(map[string]string),
		},
		{
			ops:  []int{int(utilities.OpNop), anything},
			want: make(map[string]string),
		},
		{
			ops:  []int{int(utilities.OpPush), anything},
			path: "abc",
			want: make(map[string]string),
		},
		{
			ops:  []int{int(utilities.OpPush), anything},
			verb: "LOCK",
			path: "abc:LOCK",
			want: make(map[string]string),
		},
		{
			ops:  []int{int(utilities.OpLitPush), 0},
			pool: []string{"endpoint"},
			path: "endpoint",
			want: make(map[string]string),
		},
		{
			ops:  []int{int(utilities.OpPushM), anything},
			path: "abc/def/ghi",
			want: make(map[string]string),
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 2,
			},
			pool: []string{"v1", "bucket", "name"},
			path: "v1/bucket/my-bucket",
			want: map[string]string{
				"name": "my-bucket",
			},
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 2,
			},
			pool: []string{"v1", "bucket", "name"},
			verb: "LOCK",
			path: "v1/bucket/my-bucket:LOCK",
			want: map[string]string{
				"name": "my-bucket",
			},
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPushM), anything,
				int(utilities.OpConcatN), 2,
				int(utilities.OpCapture), 2,
			},
			pool: []string{"v1", "o", "name"},
			path: "v1/o/my-bucket/dir/dir2/obj",
			want: map[string]string{
				"name": "o/my-bucket/dir/dir2/obj",
			},
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPushM), anything,
				int(utilities.OpLitPush), 2,
				int(utilities.OpConcatN), 3,
				int(utilities.OpCapture), 4,
				int(utilities.OpLitPush), 3,
			},
			pool: []string{"v1", "o", ".ext", "tail", "name"},
			path: "v1/o/my-bucket/dir/dir2/obj/.ext/tail",
			want: map[string]string{
				"name": "o/my-bucket/dir/dir2/obj/.ext",
			},
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 2,
				int(utilities.OpCapture), 2,
				int(utilities.OpLitPush), 3,
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 4,
			},
			pool: []string{"v2", "b", "name", "o", "oname"},
			path: "v2/b/my-bucket/o/obj",
			want: map[string]string{
				"name":  "b/my-bucket",
				"oname": "obj",
			},
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpPush), 0,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 1,
				int(utilities.OpLitPush), 2,
			},
			pool: []string{"foo", "id", "bar"},
			path: "foo/part1%2Fpart2/bar",
			want: map[string]string{
				"id": "part1/part2",
			},
			mode: UnescapingModeAllExceptReserved,
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpPushM), 0,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 1,
			},
			pool: []string{"foo", "id"},
			path: "foo/test%2Fbar",
			want: map[string]string{
				"id": "test%2Fbar",
			},
			mode: UnescapingModeAllExceptReserved,
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpPushM), 0,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 1,
			},
			pool: []string{"foo", "id"},
			path: "foo/test%2Fbar",
			want: map[string]string{
				"id": "test/bar",
			},
			mode: UnescapingModeAllCharacters,
		},
	} {
		pat, err := NewPattern(validVersion, spec.ops, spec.pool, spec.verb)
		if err != nil {
			t.Errorf("NewPattern(%d, %v, %q, %q) failed with %v; want success", validVersion, spec.ops, spec.pool, spec.verb, err)
			continue
		}

		components, verb := segments(spec.path)
		got, err := pat.MatchAndEscape(components, verb, spec.mode)
		if err != nil {
			t.Errorf("pat.Match(%q) failed with %v; want success; pattern = (%v, %q)", spec.path, err, spec.ops, spec.pool)
		}
		if !reflect.DeepEqual(got, spec.want) {
			t.Errorf("pat.Match(%q) = %q; want %q; pattern = (%v, %q)", spec.path, got, spec.want, spec.ops, spec.pool)
		}
	}
}

func segments(path string) (components []string, verb string) {
	if path == "" {
		return nil, ""
	}
	components = strings.Split(path, "/")
	l := len(components)
	c := components[l-1]
	if idx := strings.LastIndex(c, ":"); idx >= 0 {
		components[l-1], verb = c[:idx], c[idx+1:]
	}
	return components, verb
}

func TestPatternString(t *testing.T) {
	for _, spec := range []struct {
		ops  []int
		pool []string

		want string
	}{
		{
			want: "/",
		},
		{
			ops:  []int{int(utilities.OpNop), anything},
			want: "/",
		},
		{
			ops:  []int{int(utilities.OpPush), anything},
			want: "/*",
		},
		{
			ops:  []int{int(utilities.OpLitPush), 0},
			pool: []string{"endpoint"},
			want: "/endpoint",
		},
		{
			ops:  []int{int(utilities.OpPushM), anything},
			want: "/**",
		},
		{
			ops: []int{
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 1,
			},
			want: "/*",
		},
		{
			ops: []int{
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 0,
			},
			pool: []string{"name"},
			want: "/{name=*}",
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 2,
				int(utilities.OpCapture), 2,
				int(utilities.OpLitPush), 3,
				int(utilities.OpPushM), anything,
				int(utilities.OpLitPush), 4,
				int(utilities.OpConcatN), 3,
				int(utilities.OpCapture), 6,
				int(utilities.OpLitPush), 5,
			},
			pool: []string{"v1", "buckets", "bucket_name", "objects", ".ext", "tail", "name"},
			want: "/v1/{bucket_name=buckets/*}/{name=objects/**/.ext}/tail",
		},
	} {
		p, err := NewPattern(validVersion, spec.ops, spec.pool, "")
		if err != nil {
			t.Errorf("NewPattern(%d, %v, %q, %q) failed with %v; want success", validVersion, spec.ops, spec.pool, "", err)
			continue
		}
		if got, want := p.String(), spec.want; got != want {
			t.Errorf("%#v.String() = %q; want %q", p, got, want)
		}

		verb := "LOCK"
		p, err = NewPattern(validVersion, spec.ops, spec.pool, verb)
		if err != nil {
			t.Errorf("NewPattern(%d, %v, %q, %q) failed with %v; want success", validVersion, spec.ops, spec.pool, verb, err)
			continue
		}
		if got, want := p.String(), fmt.Sprintf("%s:%s", spec.want, verb); got != want {
			t.Errorf("%#v.String() = %q; want %q", p, got, want)
		}
	}
}

func TestMatchStrictDeepWildcard(t *testing.T) {
	for _, spec := range []struct {
		ops      []int
		pool     []string
		verb     string
		match    []string
		notMatch []string
	}{
		{
			// Mirrors grpc-gateway#5771: /v3/files/{path=**} must not match bare
			// /v3/files — the deep wildcard is preceded by two literal segments and
			// needs an actual path component of its own to bind to.
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPushM), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 2,
			},
			pool: []string{"v3", "files", "path"},
			match: []string{
				"v3/files/",
				"v3/files/a.txt",
				"v3/files/dir/a.txt",
			},
			notMatch: []string{
				"",
				"v3",
				"v3/files",
			},
		},
		{
			// Same pattern as above (/v3/files/{path=**}:get) but with a verb suffix.
			// The guard clause in OpPushM is verb-agnostic, so /v3/files:get (no
			// segment for the wildcard) must still NOT match, even with the verb suffix.
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPushM), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 2,
			},
			pool: []string{"v3", "files", "path"},
			verb: "get",
			match: []string{
				"v3/files/:get",
				"v3/files/a.txt:get",
				"v3/files/dir/a.txt:get",
			},
			notMatch: []string{
				"v3/files:get",
			},
		},
		{
			// Preceded "**" with a trailing literal ("{a}/{path=**}/edit"): needs a
			// real segment for "path", same pos>0 rule as above, just with a
			// non-empty tailLen — see https://github.com/grpc-ecosystem/grpc-gateway/issues/5771.
			ops: []int{
				int(utilities.OpPush), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 0,
				int(utilities.OpPushM), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 1,
				int(utilities.OpLitPush), 2,
			},
			pool: []string{"a", "path", "edit"},
			match: []string{
				"x/y/edit",
			},
			notMatch: []string{
				"x/edit",
			},
		},
		{
			// Bare "**" with a trailing literal ("{path=**}/edit"): nothing precedes
			// the wildcard, so zero segments is still legitimate (the pos == 0
			// exemption applies here too, not just when tailLen is zero).
			ops: []int{
				int(utilities.OpPushM), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 0,
				int(utilities.OpLitPush), 1,
			},
			pool:  []string{"path", "edit"},
			match: []string{"edit"},
		},
		{
			ops:   []int{int(utilities.OpPushM), anything},
			match: []string{"", "abc", "abc/def"},
		},
		{
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpLitPush), 1,
				int(utilities.OpPushM), anything,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 2,
			},
			pool:     []string{"v1", "o", "name"},
			match:    []string{"v1/o/my-bucket", "v1/o/a/b"},
			notMatch: []string{"v1/o"},
		},
	} {
		pat, err := NewPatternWithOptions(validVersion, spec.ops, spec.pool, spec.verb, WithStrictDeepWildcard())
		if err != nil {
			t.Errorf("NewPattern(%d, %v, %q, %q) failed with %v; want success", validVersion, spec.ops, spec.pool, spec.verb, err)
			continue
		}
		for _, path := range spec.match {
			components, verb := segments(path)
			_, err = pat.MatchAndEscape(components, verb, UnescapingModeDefault)
			if err != nil {
				t.Errorf("pat.MatchAndEscape(%q) failed with %v; want success; pattern = %v", path, err, pat)
			}
		}
		for _, path := range spec.notMatch {
			components, verb := segments(path)
			_, err = pat.MatchAndEscape(components, verb, UnescapingModeDefault)
			if err == nil {
				t.Errorf("pat.MatchAndEscape(%q) succeeded; want ErrNotMatch; pattern = %v", path, pat)
			}
		}
	}
}

func TestNewPatternInvalidLiteralSegments(t *testing.T) {
	litPush := func(operand int) []int {
		return []int{int(utilities.OpLitPush), operand}
	}
	for _, spec := range []struct {
		name        string
		ops         []int
		pool        []string
		wantSegment string
		wantLiteral string
	}{
		{
			name:        "dot dot at segment one",
			ops:         append(litPush(0), litPush(1)...),
			pool:        []string{"v1", ".."},
			wantSegment: "segment 1",
			wantLiteral: `".."`,
		},
		{
			name:        "single dot at segment zero",
			ops:         litPush(0),
			pool:        []string{"."},
			wantSegment: "segment 0",
			wantLiteral: `"."`,
		},
		{
			name: "dot after deep wildcard",
			ops: append(append(litPush(0),
				int(utilities.OpPushM), 0), litPush(1)...),
			pool:        []string{"v1", ".."},
			wantSegment: "segment 2",
			wantLiteral: `".."`,
		},
		{
			name:        "control character",
			ops:         litPush(0),
			pool:        []string{"a\x00b"},
			wantSegment: "segment 0",
			wantLiteral: "control character",
		},
		{
			name:        "invalid utf8",
			ops:         litPush(0),
			pool:        []string{"a\xffb"},
			wantSegment: "segment 0",
			wantLiteral: "invalid UTF-8",
		},
		{
			name:        "overlong literal",
			ops:         litPush(0),
			pool:        []string{strings.Repeat("a", DefaultMaxPathSegmentLength+1)},
			wantSegment: "segment 0",
			wantLiteral: "exceeds the limit",
		},
	} {
		t.Run(spec.name, func(t *testing.T) {
			_, err := NewPattern(validVersion, spec.ops, spec.pool, "")
			if err == nil {
				t.Fatalf("NewPattern with pool %v succeeded; want error", spec.pool)
			}
			if !errors.Is(err, ErrInvalidPattern) {
				t.Errorf("error %v does not wrap ErrInvalidPattern", err)
			}
			if !strings.Contains(err.Error(), spec.wantSegment) {
				t.Errorf("error %q does not contain %q", err.Error(), spec.wantSegment)
			}
			if !strings.Contains(err.Error(), spec.wantLiteral) {
				t.Errorf("error %q does not contain %q", err.Error(), spec.wantLiteral)
			}
		})
	}
}

func TestNewPatternLiteralValidationAllowsValidTemplates(t *testing.T) {
	for _, spec := range []struct {
		name string
		ops  []int
		pool []string
	}{
		{
			// The root template "/" compiles to a single empty literal.
			name: "root empty literal",
			ops:  []int{int(utilities.OpLitPush), 0},
			pool: []string{""},
		},
		{
			// Dots inside a literal ("a.b", "v2.0") are not dot segments.
			name: "literal containing dots",
			ops:  []int{int(utilities.OpLitPush), 0, int(utilities.OpLitPush), 1},
			pool: []string{"a.b", "v2.0"},
		},
		{
			// A dotted field path is an OpCapture operand, not a literal
			// segment, and must not be rejected.
			name: "dotted field path capture",
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpPush), 0,
				int(utilities.OpConcatN), 1,
				int(utilities.OpCapture), 1,
			},
			pool: []string{"v1", "id.value"},
		},
		{
			name: "deep wildcard with tail literals",
			ops: []int{
				int(utilities.OpLitPush), 0,
				int(utilities.OpPushM), 0,
				int(utilities.OpLitPush), 1,
			},
			pool: []string{"v1", "tail"},
		},
	} {
		t.Run(spec.name, func(t *testing.T) {
			pat, err := NewPattern(validVersion, spec.ops, spec.pool, "")
			if err != nil {
				t.Fatalf("NewPattern with %v failed: %v", spec.pool, err)
			}
			// MustPattern returns the same pattern on success. Its failure
			// branch calls grpclog.Fatalf (os.Exit) and cannot be exercised
			// in-process; the NewPattern error cases above cover every
			// rejection that MustPattern would fatally report.
			if got := MustPattern(pat, nil); len(got.ops) != len(pat.ops) {
				t.Errorf("MustPattern changed the pattern: %v vs %v", got, pat)
			}
		})
	}
}

func TestValidatePatternLiteral(t *testing.T) {
	for _, tc := range []struct {
		lit     string
		segment int
		wantErr bool
	}{
		{".", 0, true},
		{"..", 3, true},
		{"\x7f", 1, true},
		{"\xff", 0, true},
		{strings.Repeat("x", DefaultMaxPathSegmentLength+1), 2, true},
		{"", 0, false},
		{"a.b", 0, false},
		{strings.Repeat("x", DefaultMaxPathSegmentLength), 0, false},
	} {
		err := validatePatternLiteral(tc.lit, tc.segment)
		if tc.wantErr {
			if err == nil || !errors.Is(err, ErrInvalidPattern) {
				t.Errorf("validatePatternLiteral(%q, %d) = %v; want ErrInvalidPattern", tc.lit, tc.segment, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("validatePatternLiteral(%q, %d) = %v; want nil", tc.lit, tc.segment, err)
		}
	}
}

// TestMustPatternFatalOnInvalidLiteral exercises MustPattern's fatal branch
// in a re-executed child process: grpclog.Fatalf terminates with os.Exit(1)
// when the wrapped pattern construction fails.
func TestMustPatternFatalOnInvalidLiteral(t *testing.T) {
	const envVar = "GRPC_GATEWAY_MUSTPATTERN_FATAL_CHILD"
	if os.Getenv(envVar) == "1" {
		MustPattern(NewPattern(validVersion,
			[]int{int(utilities.OpLitPush), 0}, []string{".."}, ""))
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMustPatternFatalOnInvalidLiteral$")
	cmd.Env = append(os.Environ(), envVar+"=1")
	err := cmd.Run()
	var exitErr *exec.ExitError
	if err == nil {
		t.Fatal("child process exited successfully; want non-zero exit for invalid pattern")
	}
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("child error = %v; want exit status 1", err)
	}
}
