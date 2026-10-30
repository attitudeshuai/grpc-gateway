package runtime

import (
	"context"
	"fmt"
	"mime"
	"net/http"

	"google.golang.org/protobuf/proto"
)

// This file holds the construction-time resolution of ServeMux options.
//
// Every option is parsed while NewServeMux is running. The result is a single
// muxConfig snapshot that never changes afterwards and is shared by every
// concurrent request; request handlers only read it, they never recompute it.
//
// The resolution rules are deliberately order-independent:
//
//   - Toggle options (the zero-argument "With..." switches) only ever turn a
//     behavior on. Declaring the same toggle once, twice or in any position
//     relative to other options yields the same snapshot.
//   - List options keep their append semantics; the declaration order is the
//     execution order by design.
//   - A single-valued option (an enum or a hook) can be declared at most once
//     with a non-default value. Two declarations that cannot both be honored
//     make NewServeMux panic and name the two conflicting options, so a
//     half-configured ServeMux is never returned.
//
// Coupled switches that are not contradictory take effect according to the
// priorities documented on writeChunkMarker and writeContentLengthHeader.

// muxOptionSlot identifies a single-valued option that must not be declared
// twice with conflicting values.
type muxOptionSlot int

const (
	slotUnescapingMode muxOptionSlot = iota
	slotForwardResponseRewriter
	slotIncomingHeaderMatcher
	slotOutgoingHeaderMatcher
	slotOutgoingTrailerMatcher
	slotErrorHandler
	slotStreamErrorHandler
	slotRoutingErrorHandler
	slotQueryParameterParser
)

// muxConfig is the immutable snapshot of the resolved ServeMux switches.
// Instances are populated exclusively through ServeMux option setters while
// NewServeMux runs and are frozen before the ServeMux is returned. All fields
// are read-only after that point, so concurrent requests may share the same
// pointer without synchronization.
type muxConfig struct {
	// Response framing switches.
	disableChunkedEncoding bool
	writeContentLength     bool

	// Request routing switches.
	disablePathLengthFallback bool
	disableHTTPMethodOverride bool
	unescapingMode            UnescapingMode
	strictDeepWildcard        bool

	// Resolved single-valued hooks (defaults applied at freeze time).
	forwardResponseRewriter ForwardResponseRewriter
	incomingHeaderMatcher   HeaderMatcherFunc
	outgoingHeaderMatcher   HeaderMatcherFunc
	outgoingTrailerMatcher  HeaderMatcherFunc
	errorHandler            ErrorHandlerFunc
	streamErrorHandler      StreamErrorHandlerFunc
	routingErrorHandler     RoutingErrorHandlerFunc
}

// mutate runs an option body while NewServeMux is still collecting options.
// Applying an option once construction has finished would mutate the shared
// snapshot, so it is rejected instead.
func (m *ServeMux) mutate(option string, apply func()) {
	if m.frozen {
		panic(fmt.Errorf("runtime: %s applied after NewServeMux returned; ServeMux options can only be passed to NewServeMux", option))
	}
	apply()
}

// declareScalar records a declaration of a single-valued option. Declaring the
// same slot twice is an order-dependent contradiction, so construction fails
// and the error names the two conflicting options.
func (m *ServeMux) declareScalar(slot muxOptionSlot, option string, apply func()) {
	m.mutate(option, func() {
		if previous, ok := m.declared[slot]; ok {
			panic(conflictingOptionsError(previous, option))
		}
		m.declared[slot] = option
		apply()
	})
}

// setUnescapingMode resolves WithUnescapingMode. Repeating the same mode is
// the idempotent declaration of one value and stays order-independent; two
// different modes cannot both be honored.
func (m *ServeMux) setUnescapingMode(mode UnescapingMode) {
	option := "WithUnescapingMode(" + unescapingModeName(mode) + ")"
	m.mutate(option, func() {
		if mode < UnescapingModeLegacy || mode > UnescapingModeAllCharacters {
			panic(fmt.Errorf("runtime: invalid value %d for WithUnescapingMode; valid values are UnescapingModeLegacy through UnescapingModeAllCharacters", mode))
		}
		if previous, ok := m.declared[slotUnescapingMode]; ok {
			if mode != m.cfg.unescapingMode {
				panic(conflictingOptionsError(previous, option))
			}
			return
		}
		m.declared[slotUnescapingMode] = option
		m.cfg.unescapingMode = mode
	})
}

func unescapingModeName(mode UnescapingMode) string {
	switch mode {
	case UnescapingModeLegacy:
		return "UnescapingModeLegacy"
	case UnescapingModeAllExceptReserved:
		return "UnescapingModeAllExceptReserved"
	case UnescapingModeAllExceptSlash:
		return "UnescapingModeAllExceptSlash"
	case UnescapingModeAllCharacters:
		return "UnescapingModeAllCharacters"
	default:
		return fmt.Sprintf("UnescapingMode(%d)", int(mode))
	}
}

func conflictingOptionsError(first, second string) error {
	return fmt.Errorf(
		"runtime: incompatible ServeMux options %s and %s: the two switches cannot take effect together; remove one of them",
		first, second,
	)
}

// freeze fills defaults and marks the resolved snapshot immutable. It runs
// exactly once, after every option has been parsed and before the ServeMux is
// returned or any route registered by an option is observable.
func (m *ServeMux) freeze() {
	if m.cfg.forwardResponseRewriter == nil {
		m.cfg.forwardResponseRewriter = func(_ context.Context, response proto.Message) (any, error) {
			return response, nil
		}
	}
	if m.cfg.incomingHeaderMatcher == nil {
		m.cfg.incomingHeaderMatcher = DefaultHeaderMatcher
	}
	if m.cfg.outgoingHeaderMatcher == nil {
		m.cfg.outgoingHeaderMatcher = defaultOutgoingHeaderMatcher
	}
	if m.cfg.outgoingTrailerMatcher == nil {
		m.cfg.outgoingTrailerMatcher = defaultOutgoingTrailerMatcher
	}
	if m.cfg.errorHandler == nil {
		m.cfg.errorHandler = DefaultHTTPErrorHandler
	}
	if m.cfg.streamErrorHandler == nil {
		m.cfg.streamErrorHandler = DefaultStreamErrorHandler
	}
	if m.cfg.routingErrorHandler == nil {
		m.cfg.routingErrorHandler = DefaultRoutingErrorHandler
	}
	if m.queryParser != nil {
		currentQueryParser = m.queryParser
	}
	m.declared = nil
	m.frozen = true
}

// isFormContentType reports whether the request carries the form content type
// (ignoring parameters such as charset), exactly like net/http's ParseForm.
func isFormContentType(r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && mediaType == "application/x-www-form-urlencoded"
}

// writeChunkMarker reports whether the resolved policy allows writing the
// "Transfer-Encoding: chunked" marker for a response.
//
// Only streaming responses and buffered responses that have to carry trailers
// use chunked framing. The priority is fixed and construction-time resolved:
// WithDisableChunkedEncoding wins over every other consideration, including
// trailer forwarding, so no request path (unary, streaming or error) writes
// the marker once chunked encoding is disabled.
func (c *muxConfig) writeChunkMarker(streaming, forwardsTrailers bool) bool {
	if c.disableChunkedEncoding {
		return false
	}
	return streaming || forwardsTrailers
}

// writeContentLengthHeader reports whether "Content-Length" is written.
//
// Length is only known for buffered (non-streaming) responses. Per RFC 7230
// Content-Length and Transfer-Encoding/chunked trailers are mutually
// exclusive, so a response that carries trailers deterministically gets no
// Content-Length even when WithWriteContentLength is declared: trailers take
// priority over length. This rule is identical on the unary and error paths.
func (c *muxConfig) writeContentLengthHeader(streaming, forwardsTrailers bool) bool {
	return c.writeContentLength && !streaming && !forwardsTrailers
}

// methodOverrideAllowed reports whether the X-HTTP-Method-Override header is
// honored for the request. Its result is determined by the method-override
// switch alone (plus the request being a form POST, which is the only request
// shape the override has ever applied to): it no longer silently disappears
// when WithDisablePathLengthFallback is also declared.
func (c *muxConfig) methodOverrideAllowed(r *http.Request) bool {
	if c.disableHTTPMethodOverride {
		return false
	}
	return r.Method == http.MethodPost && isFormContentType(r)
}
