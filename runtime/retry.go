package runtime

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RetryOutcome classifies how a unary upstream call finished when a retry
// policy applies to it.
type RetryOutcome int

const (
	// RetryOutcomeFirstAttemptSucceeded means the upstream call succeeded on
	// the first attempt; no retry happened.
	RetryOutcomeFirstAttemptSucceeded RetryOutcome = iota
	// RetryOutcomeRetriedSucceeded means at least one retryable failure
	// happened and the call succeeded on a later attempt.
	RetryOutcomeRetriedSucceeded
	// RetryOutcomeBudgetExhausted means retrying stopped because the declared
	// maximum number of attempts was reached, or the remaining total budget
	// (the client deadline or the locally declared total timeout, whichever
	// is tighter) was insufficient for another attempt.
	RetryOutcomeBudgetExhausted
	// RetryOutcomeNonRetryableError means the upstream failure is not
	// retryable (for example InvalidArgument, PermissionDenied or NotFound);
	// the failure was mapped back to the client immediately.
	RetryOutcomeNonRetryableError
	// RetryOutcomeAborted means retrying was abandoned because the client
	// disconnected, or the surrounding context was canceled while waiting to
	// retry. No further attempt was started.
	RetryOutcomeAborted
)

// RetryStats describes the result of a policy-controlled unary upstream call.
// It is delivered to the RetryObserver configured with WithRetryObserver.
type RetryStats struct {
	// Method is the fully qualified upstream method, in the form
	// "/package.Service/Method".
	Method string
	// Attempts is the number of upstream attempts that were actually started.
	Attempts int
	// Outcome classifies the final result.
	Outcome RetryOutcome
	// LastError is the error returned by the last attempt, or nil when the
	// call succeeded.
	LastError error
}

// RetryObserver observes the outcome of a policy-controlled unary upstream
// call. It is called exactly once per incoming request that matches a
// declared retry policy, after the upstream interaction has finished.
// Observers must not block and must not modify the arguments.
type RetryObserver func(RetryStats)

// BackoffStrategy decides how long to wait before a retry attempt.
// Attempt is the 1-based number of the attempt about to be started, so the
// wait before the second attempt is requested with attempt == 2. Returning a
// non-positive duration skips the wait.
type BackoffStrategy interface {
	NextBackoff(attempt int) time.Duration
}

// ExponentialBackoff is a deterministic (jitter-free) exponential BackoffStrategy.
// The wait before attempt n is InitialInterval * Multiplier**(n-2), capped at
// MaxInterval.
type ExponentialBackoff struct {
	// InitialInterval is the wait before the second attempt. It defaults to
	// 100ms when zero.
	InitialInterval time.Duration
	// MaxInterval caps the wait between attempts. It defaults to 10s when
	// zero.
	MaxInterval time.Duration
	// Multiplier is applied to the interval after every failed attempt. It
	// defaults to 2.0 when not greater than 1.
	Multiplier float64
}

// NextBackoff implements BackoffStrategy.
func (b ExponentialBackoff) NextBackoff(attempt int) time.Duration {
	initial := b.InitialInterval
	if initial <= 0 {
		initial = 100 * time.Millisecond
	}
	max := b.MaxInterval
	if max <= 0 {
		max = 10 * time.Second
	}
	multiplier := b.Multiplier
	if multiplier <= 1 {
		multiplier = 2
	}
	steps := attempt - 2
	if steps < 0 {
		steps = 0
	}
	next := float64(initial) * math.Pow(multiplier, float64(steps))
	if next >= float64(max) || math.IsInf(next, 1) {
		return max
	}
	return time.Duration(next)
}

// RetryPolicy declares whether and how unary upstream calls are retried.
//
// A policy only ever applies to the target it is declared for (a service or
// a method); without a declaration no retry happens and the upstream call,
// error mapping and response metadata handling are unchanged.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts allowed, including the
	// first one. Values below 2 disable retrying for the target.
	MaxAttempts int
	// TotalTimeout optionally caps the total wall-clock time available across
	// all attempts. The effective budget is the tighter of this cap and the
	// deadline carried by the incoming context (the Grpc-Timeout header).
	// Zero means there is no local cap.
	TotalTimeout time.Duration
	// Codes lists the gRPC status codes that are worth retrying. It defaults
	// to only codes.Unavailable when empty. Failures with any other code,
	// such as InvalidArgument, PermissionDenied or NotFound, fail the request
	// immediately and are mapped using the existing error mapping.
	Codes []codes.Code
	// Backoff selects the wait between attempts. It defaults to
	// ExponentialBackoff with its default parameters when nil.
	Backoff BackoffStrategy
}

// UnaryCallFunc performs a single attempt of a unary upstream call. Every
// invocation receives an independent context and independent gRPC header and
// trailer collectors (passed as opts by InvokeUnary).
type UnaryCallFunc func(ctx context.Context, opts ...grpc.CallOption) (proto.Message, error)

// InvokeUnary runs a unary upstream call, retrying it according to the retry
// policy declared on the ServeMux for fullMethod.
//
// When no policy applies to fullMethod (the default), InvokeUnary performs a
// single call on ctx with exactly the same call options the generated code
// used historically (grpc.Header and grpc.Trailer bound to the returned
// ServerMetadata), so the observable behavior is unchanged.
//
// When a policy applies, attempts run strictly serially and share one total
// budget: the incoming context deadline (Grpc-Timeout) and the policy's
// TotalTimeout, tightened to the earlier one. Each attempt gets its own
// context and its own header/trailer metadata; only the metadata of the final
// attempt is returned, so metadata collected by failed attempts cannot leak
// into the response. The wait before a retry is canceled when the client
// disconnects, the budget expires or the surrounding context is canceled.
func InvokeUnary(ctx context.Context, fullMethod string, call UnaryCallFunc) (proto.Message, ServerMetadata, error) {
	var md ServerMetadata

	retryCtx, hasRetryCtx := retryContextFromContext(ctx)
	var policy retryPolicy
	hasPolicy := false
	if hasRetryCtx {
		policy, hasPolicy = retryCtx.policies.lookup(fullMethod)
	}
	if !hasPolicy || policy.maxAttempts < 2 {
		msg, err := call(ctx, grpc.Header(&md.HeaderMD), grpc.Trailer(&md.TrailerMD))
		return msg, md, err
	}

	return invokeUnaryWithRetry(ctx, fullMethod, policy, retryCtx, call)
}

func invokeUnaryWithRetry(parent context.Context, fullMethod string, policy retryPolicy, retryCtx retryContextData, call UnaryCallFunc) (proto.Message, ServerMetadata, error) {
	start := time.Now()
	deadline, hasDeadline := parent.Deadline()
	if policy.totalTimeout > 0 {
		localDeadline := start.Add(policy.totalTimeout)
		if !hasDeadline || localDeadline.Before(deadline) {
			deadline = localDeadline
			hasDeadline = true
		}
	}

	budgetCtx := parent
	cancelBudget := func() {}
	if hasDeadline {
		budgetCtx, cancelBudget = context.WithDeadline(parent, deadline)
	}
	defer cancelBudget()

	var (
		msg      proto.Message
		md       ServerMetadata
		lastErr  error
		attempts int
		outcome  RetryOutcome
	)

	for {
		attempts++

		// Each attempt uses an independent context and independent
		// header/trailer collectors.
		attemptCtx, cancelAttempt := context.WithCancel(budgetCtx)
		var attemptMD ServerMetadata
		msg, lastErr = call(attemptCtx, grpc.Header(&attemptMD.HeaderMD), grpc.Trailer(&attemptMD.TrailerMD))
		cancelAttempt()
		md = attemptMD

		if lastErr == nil {
			if attempts == 1 {
				outcome = RetryOutcomeFirstAttemptSucceeded
			} else {
				outcome = RetryOutcomeRetriedSucceeded
			}
			retryCtx.observer(RetryStats{Method: fullMethod, Attempts: attempts, Outcome: outcome})
			return msg, md, nil
		}

		if attempts >= policy.maxAttempts {
			outcome = RetryOutcomeBudgetExhausted
			break
		}
		if !isRetryableStatus(lastErr, policy) {
			outcome = RetryOutcomeNonRetryableError
			break
		}

		// A disconnected client must not trigger any further attempts, and we
		// must not block in backoff waiting for a client that is already gone.
		if retryCtx.clientDone != nil {
			select {
			case <-retryCtx.clientDone:
				outcome = RetryOutcomeAborted
				retryCtx.observer(RetryStats{Method: fullMethod, Attempts: attempts, Outcome: outcome, LastError: lastErr})
				return nil, md, lastErr
			default:
			}
		}

		wait := policy.backoff.NextBackoff(attempts + 1)
		if hasDeadline {
			remaining := time.Until(deadline)
			// Not enough budget left to even finish the backoff, so another
			// attempt cannot fit in the budget.
			if remaining <= wait {
				outcome = RetryOutcomeBudgetExhausted
				break
			}
		}

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-retryCtx.clientDone:
			stopRetryTimer(timer)
			outcome = RetryOutcomeAborted
			retryCtx.observer(RetryStats{Method: fullMethod, Attempts: attempts, Outcome: outcome, LastError: lastErr})
			return nil, md, lastErr
		case <-budgetCtx.Done():
			stopRetryTimer(timer)
			if hasDeadline && !time.Now().Before(deadline) {
				outcome = RetryOutcomeBudgetExhausted
			} else {
				outcome = RetryOutcomeAborted
			}
			retryCtx.observer(RetryStats{Method: fullMethod, Attempts: attempts, Outcome: outcome, LastError: lastErr})
			return nil, md, lastErr
		}
	}

	retryCtx.observer(RetryStats{Method: fullMethod, Attempts: attempts, Outcome: outcome, LastError: lastErr})
	// Only the last attempt's error and metadata are returned: the existing
	// error mapping decides status and body, and earlier header/trailer
	// metadata never reaches the response.
	return nil, md, lastErr
}

func isRetryableStatus(err error, policy retryPolicy) bool {
	if len(policy.codes) == 0 {
		return status.Code(err) == codes.Unavailable
	}
	_, ok := policy.codes[status.Code(err)]
	return ok
}

func stopRetryTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

// retryPolicy is the normalized form of RetryPolicy.
type retryPolicy struct {
	maxAttempts  int
	totalTimeout time.Duration
	codes        map[codes.Code]struct{}
	backoff      BackoffStrategy
}

// retryPolicySet resolves the policy for a fully qualified upstream method.
// Exact method declarations take precedence over service-wide declarations.
type retryPolicySet struct {
	methods  map[string]retryPolicy
	services map[string]retryPolicy
}

func newRetryPolicySet(entries []retryPolicyEntry) (*retryPolicySet, error) {
	set := &retryPolicySet{
		methods:  make(map[string]retryPolicy),
		services: make(map[string]retryPolicy),
	}
	for _, entry := range entries {
		target := entry.target
		if !strings.HasPrefix(target, "/") {
			return nil, errInvalidRetryTarget(target)
		}
		policy := normalizeRetryPolicy(entry.policy)
		switch strings.Count(target, "/") {
		case 1:
			// "/package.Service"
			set.services[target] = policy
		default:
			// "/package.Service/Method"
			set.methods[target] = policy
		}
	}
	return set, nil
}

func normalizeRetryPolicy(p RetryPolicy) retryPolicy {
	normalized := retryPolicy{
		maxAttempts:  p.MaxAttempts,
		totalTimeout: p.TotalTimeout,
		backoff:      p.Backoff,
	}
	if normalized.backoff == nil {
		normalized.backoff = ExponentialBackoff{}
	}
	if len(p.Codes) > 0 {
		normalized.codes = make(map[codes.Code]struct{}, len(p.Codes))
		for _, code := range p.Codes {
			normalized.codes[code] = struct{}{}
		}
	}
	return normalized
}

func (s *retryPolicySet) lookup(fullMethod string) (retryPolicy, bool) {
	if policy, ok := s.methods[fullMethod]; ok {
		return policy, true
	}
	if i := strings.LastIndex(fullMethod, "/"); i > 0 {
		if policy, ok := s.services[fullMethod[:i]]; ok {
			return policy, true
		}
	}
	return retryPolicy{}, false
}

type retryPolicyEntry struct {
	target string
	policy RetryPolicy
}

func errInvalidRetryTarget(target string) error {
	return fmt.Errorf("invalid retry policy target %q: want \"/package.Service\" or \"/package.Service/Method\"", target)
}

// retryContextData carries the retry configuration into the generated request
// function. It is attached by AnnotateContext.
type retryContextData struct {
	policies   *retryPolicySet
	observer   RetryObserver
	clientDone <-chan struct{}
}

type retryContextKey struct{}

func withRetryContext(ctx context.Context, data retryContextData) context.Context {
	return context.WithValue(ctx, retryContextKey{}, data)
}

func retryContextFromContext(ctx context.Context) (retryContextData, bool) {
	data, ok := ctx.Value(retryContextKey{}).(retryContextData)
	return data, ok
}
