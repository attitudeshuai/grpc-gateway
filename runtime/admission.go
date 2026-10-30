package runtime

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdmissionRule declares request admission limits for a gRPC service or a
// single method.
//
// A rule is identified the same way requests are: Service is the fully
// qualified gRPC service name without a leading slash (for example
// "grpc.health.v1.Health") and Method is the simple method name (for example
// "Check"). An empty Method makes the rule apply to every method of the
// service that does not declare its own rule.
//
// MaxConcurrent bounds how many admitted requests may be executing upstream
// calls at the same time. MaxQueued bounds how many requests may wait for an
// execution slot; a request that finds every slot occupied and the queue full
// is rejected before its request is assembled or forwarded. QueueTimeout
// bounds how long a request may wait in the queue; on timeout the request is
// rejected, again without ever contacting the upstream.
type AdmissionRule struct {
	// Service is the fully qualified gRPC service name, e.g. "my.Service".
	Service string
	// Method is the simple method name, e.g. "Echo". Empty means the rule
	// applies to all of Service's methods unless a method-specific rule exists.
	Method string
	// MaxConcurrent is the maximum number of requests executing upstream
	// calls simultaneously. It must be greater than zero.
	MaxConcurrent int
	// MaxQueued is the maximum number of requests waiting for a slot. It must
	// be zero or greater. Zero rejects immediately when all slots are taken.
	MaxQueued int
	// QueueTimeout bounds the time a request may wait in the queue. It must be
	// greater than zero when MaxQueued is greater than zero.
	QueueTimeout time.Duration
}

// Sentinel errors produced by the gateway's own admission process.
var (
	// ErrAdmissionQueueFull is returned when no execution slot is available
	// and the rule's waiting queue has reached its declared capacity. The
	// request is rejected with HTTP 429 (gRPC ResourceExhausted).
	ErrAdmissionQueueFull = errors.New("admission: concurrency limit reached and queue is full")
	// ErrAdmissionQueueTimeout is returned when a queued request could not get
	// an execution slot within the rule's QueueTimeout. The request is
	// rejected with HTTP 503 (gRPC Unavailable); it never reached the upstream.
	ErrAdmissionQueueTimeout = errors.New("admission: timed out waiting for an execution slot")
)

// AdmissionController holds the immutable snapshot of admission rules and the
// metered state used to enforce them. A controller is built once, when the
// ServeMux is constructed, and never changes afterwards: rule lookup only
// reads the construction-time snapshot.
type AdmissionController struct {
	// methodRules indexes method-specific rules by gRPC full method name
	// ("/package.Service/Method"). serviceRules indexes service-wide rules by
	// the fully qualified service name. Both maps are immutable after New.
	methodRules  map[string]*admissionLimiter
	serviceRules map[string]*admissionLimiter
}

// NewAdmissionController builds an AdmissionController from rules.
//
// Every rule is validated first and the controller is only published when all
// of them are valid, so a failure never leaves a half-built rule table.
// Duplicate service-level or method-level rules, unknown limits and rule
// identities that are not valid gRPC names are reported as errors.
func NewAdmissionController(rules ...AdmissionRule) (*AdmissionController, error) {
	methodRules := make(map[string]*admissionLimiter, len(rules))
	serviceRules := make(map[string]*admissionLimiter, len(rules))
	for i, rule := range rules {
		if err := validateAdmissionRule(rule); err != nil {
			return nil, fmt.Errorf("invalid admission rule %d: %w", i, err)
		}
		l := newAdmissionLimiter(rule)
		if rule.Method == "" {
			if _, dup := serviceRules[rule.Service]; dup {
				return nil, fmt.Errorf("invalid admission rule %d: duplicate service rule for %q", i, rule.Service)
			}
			serviceRules[rule.Service] = l
			continue
		}
		fullMethod := "/" + rule.Service + "/" + rule.Method
		if _, dup := methodRules[fullMethod]; dup {
			return nil, fmt.Errorf("invalid admission rule %d: duplicate method rule for %q", i, fullMethod)
		}
		methodRules[fullMethod] = l
	}
	return &AdmissionController{methodRules: methodRules, serviceRules: serviceRules}, nil
}

// MustAdmissionController is like NewAdmissionController but panics when the
// rules are invalid. It is intended for statically known rule sets, mirroring
// MustPattern for route patterns.
func MustAdmissionController(rules ...AdmissionRule) *AdmissionController {
	ac, err := NewAdmissionController(rules...)
	if err != nil {
		panic(err)
	}
	return ac
}

// limiterFor returns the limiter that applies to fullMethod (a gRPC full
// method name of the form "/package.Service/Method"). A method-specific rule
// takes precedence over the service-wide rule. It returns nil when no rule
// applies, in which case admission is a pure pass-through.
func (c *AdmissionController) limiterFor(fullMethod string) *admissionLimiter {
	if c == nil {
		return nil
	}
	if l, ok := c.methodRules[fullMethod]; ok {
		return l
	}
	if service, _, ok := splitFullMethod(fullMethod); ok {
		if l, ok := c.serviceRules[service]; ok {
			return l
		}
	}
	return nil
}

// AdmissionStats reports the metered state of the rule applying to fullMethod.
// running is the number of requests holding an execution slot, waiting is the
// number of requests currently queued, and ok reports whether any rule
// applies. The snapshot is taken from the same counters acquire/release use.
func (c *AdmissionController) AdmissionStats(fullMethod string) (running, waiting int, ok bool) {
	l := c.limiterFor(fullMethod)
	if l == nil {
		return 0, 0, false
	}
	return l.stats()
}

func validateAdmissionRule(rule AdmissionRule) error {
	if rule.Service == "" {
		return errors.New("Service must not be empty")
	}
	if strings.HasPrefix(rule.Service, "/") {
		return fmt.Errorf("Service must not start with %q: %q", "/", rule.Service)
	}
	if strings.ContainsAny(rule.Service, " \t\r\n") {
		return fmt.Errorf("Service must not contain whitespace: %q", rule.Service)
	}
	if strings.Contains(rule.Method, "/") {
		return fmt.Errorf("Method must be a simple method name: %q", rule.Method)
	}
	if rule.MaxConcurrent <= 0 {
		return fmt.Errorf("MaxConcurrent must be greater than zero, got %d", rule.MaxConcurrent)
	}
	if rule.MaxQueued < 0 {
		return fmt.Errorf("MaxQueued must be zero or greater, got %d", rule.MaxQueued)
	}
	if rule.MaxQueued > 0 && rule.QueueTimeout <= 0 {
		return errors.New("QueueTimeout must be greater than zero when MaxQueued is greater than zero")
	}
	return nil
}

// splitFullMethod splits a gRPC full method name "/package.Service/Method"
// into the service name and the simple method name.
func splitFullMethod(fullMethod string) (service, method string, ok bool) {
	if !strings.HasPrefix(fullMethod, "/") {
		return "", "", false
	}
	rest := fullMethod[1:]
	idx := strings.LastIndex(rest, "/")
	if idx <= 0 || idx == len(rest)-1 {
		return "", "", false
	}
	return rest[:idx], rest[idx+1:], true
}

// admissionWaiter represents one request waiting in the FIFO queue.
type admissionWaiter struct {
	// ch is closed when the waiter is granted a slot.
	ch chan struct{}
	// granted records that the slot handoff already happened under the
	// limiter's lock, so a racing cancellation or timeout cannot drop a slot.
	granted bool
}

// admissionLimiter enforces one AdmissionRule. All counter and queue mutations
// happen under mu, which keeps running in [0, MaxConcurrent] and waiting in
// [0, MaxQueued].
type admissionLimiter struct {
	rule    AdmissionRule
	mu      sync.Mutex
	running int
	waiters *list.List
}

func newAdmissionLimiter(rule AdmissionRule) *admissionLimiter {
	return &admissionLimiter{
		rule:    rule,
		waiters: list.New(),
	}
}

// acquire implements the two admission phases:
//
//  1. If an execution slot is free, it is occupied immediately.
//  2. Otherwise, when the queue has room, the request is appended to the FIFO
//     queue and blocks until it is granted a slot, ctx is canceled, or the
//     rule's QueueTimeout elapses. When the queue is full, ErrAdmissionQueueFull
//     is returned without blocking.
//
// The returned release function must be called exactly once when the upstream
// call ends (normally, with an error, or because it was canceled). For
// streaming calls it must be called when the stream ends, so the whole stream
// occupies a single slot.
func (l *admissionLimiter) acquire(ctx context.Context) (func(), error) {
	l.mu.Lock()
	if l.running < l.rule.MaxConcurrent {
		l.running++
		l.mu.Unlock()
		return l.release, nil
	}
	if l.waiters.Len() >= l.rule.MaxQueued {
		l.mu.Unlock()
		return nil, ErrAdmissionQueueFull
	}
	w := &admissionWaiter{ch: make(chan struct{})}
	element := l.waiters.PushBack(w)
	l.mu.Unlock()

	var timer *time.Timer
	var timeout <-chan time.Time
	if l.rule.QueueTimeout > 0 {
		timer = time.NewTimer(l.rule.QueueTimeout)
		timeout = timer.C
	}

	select {
	case <-w.ch:
		if timer != nil {
			timer.Stop()
		}
		return l.release, nil
	case <-ctx.Done():
		if timer != nil {
			timer.Stop()
		}
		l.mu.Lock()
		if w.granted {
			// The slot handoff raced with cancellation: it is already counted
			// as running, so pass it on rather than leaking it.
			l.handoffLocked()
		} else {
			l.waiters.Remove(element)
		}
		l.mu.Unlock()
		return nil, ctx.Err()
	case <-timeout:
		l.mu.Lock()
		if w.granted {
			// Granted at the same instant the timeout fired: the caller
			// treats the request as rejected, so hand the slot to the head of
			// the queue instead of leaking or double-using it.
			l.handoffLocked()
		} else {
			l.waiters.Remove(element)
		}
		l.mu.Unlock()
		return nil, ErrAdmissionQueueTimeout
	}
}

// release frees one execution slot and, when requests are queued, hands it
// directly to the longest-waiting one, preserving arrival order.
func (l *admissionLimiter) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.handoffLocked()
}

// handoffLocked frees the caller's slot and grants it to the head waiter.
// Callers must hold l.mu.
func (l *admissionLimiter) handoffLocked() {
	l.running--
	if l.running < 0 {
		panic("admission: running slot counter underflow")
	}
	if next := l.waiters.Front(); next != nil {
		w := next.Value.(*admissionWaiter)
		l.waiters.Remove(next)
		w.granted = true
		l.running++
		close(w.ch)
	}
}

func (l *admissionLimiter) stats() (running, waiting int, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.running, l.waiters.Len(), true
}

// Admit wraps next so that requests for fullMethod enter through the
// ServeMux's admission controller. fullMethod uses the gRPC form
// "/package.Service/Method", the same identity generated registrations carry
// and AnnotateContext uses.
//
// When no rule is declared for fullMethod, Admit is a pure pass-through: next
// runs directly, with no slot, no queue and no rejection, so status codes,
// headers and bodies stay byte for byte what they are without admission.
//
// Rejections happen before next runs, i.e. before request assembly and the
// upstream call: a queued-full request gets HTTP 429 with Retry-After, and a
// queue timeout gets HTTP 503 with Retry-After. The granted slot is occupied
// until next returns, which for streaming handlers means until the stream
// ends; client disconnects and early upstream termination end next and return
// the slot the same way a normal response does.
func (s *ServeMux) Admit(fullMethod string, next HandlerFunc) HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
		limiter := s.admission.limiterFor(fullMethod)
		if limiter == nil {
			next(w, r, pathParams)
			return
		}
		release, err := limiter.acquire(r.Context())
		if err != nil {
			s.writeAdmissionError(w, r, limiter, err)
			return
		}
		defer release()
		next(w, r, pathParams)
	}
}

// writeAdmissionError renders the deterministic rejection produced by the
// gateway itself. Cancellation while queued answers nothing: the client that
// went away cannot read a response anyway.
func (s *ServeMux) writeAdmissionError(w http.ResponseWriter, r *http.Request, limiter *admissionLimiter, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	_, outboundMarshaler := MarshalerForRequest(s, r)

	var st *status.Status
	switch {
	case errors.Is(err, ErrAdmissionQueueTimeout):
		st = status.New(codes.Unavailable, "grpc-gateway: admission queue wait timeout exceeded; retry later")
		w.Header().Set("Retry-After", "0")
	default:
		st = status.New(codes.ResourceExhausted, "grpc-gateway: concurrent request limit reached and admission queue is full; retry later")
		w.Header().Set("Retry-After", admissionRetryAfter(limiter.rule))
	}
	s.errorHandler(r.Context(), s, outboundMarshaler, w, r, st.Err())
}

// admissionRetryAfter suggests waiting at least one second, or for the queue
// timeout window when that is longer, before retrying.
func admissionRetryAfter(rule AdmissionRule) string {
	secs := int((rule.QueueTimeout + time.Second - 1) / time.Second)
	if secs < 1 {
		secs = 1
	}
	return strconv.Itoa(secs)
}

// WithAdmissionController configures the ServeMux to enforce the given
// admission rules. The controller is the construction-time snapshot: it is
// read for every request but never mutated afterwards.
func WithAdmissionController(controller *AdmissionController) ServeMuxOption {
	return func(serveMux *ServeMux) {
		serveMux.admission = controller
	}
}

// WithAdmissionRules is a shorthand for WithAdmissionController over
// NewAdmissionController. Because ServeMuxOptions cannot report errors, it
// panics on invalid rules; prefer building the controller explicitly with
// NewAdmissionController when rules are dynamic.
func WithAdmissionRules(rules ...AdmissionRule) ServeMuxOption {
	return WithAdmissionController(MustAdmissionController(rules...))
}
