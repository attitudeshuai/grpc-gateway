package runtime

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

// registrationStateKey stores the active transactional registration in the
// context passed through generated registration functions, so that
// RegisterXHandlerFromEndpoint -> RegisterXHandler -> RegisterXHandlerClient
// participate in a single transaction even though each layer opens a scope.
type registrationStateKey struct{}

// registrationState is one transactional registration. It snapshots the routes
// mounted beforehand (so they can be rolled back on failure) and remembers the
// upstream connection acquired for the group.
type registrationState struct {
	mux      *ServeMux
	ctx      context.Context
	snapshot map[string]int
	depth    int

	// Connection acquired from the gateway-owned UpstreamConns.
	acquired *upstreamConn
	// Connection dialed for a standalone registration when the mux owns no
	// UpstreamConns; kept separate so the historical private-connection
	// behavior is byte-for-byte identical.
	legacyConn     *grpc.ClientConn
	legacyEndpoint string
}

// BeginRegistration opens a transactional registration scope on the mux.
//
// Generated registration functions call it first; the returned finish function
// is deferred with a pointer to the call's named error. When the registration
// returns an error (or panics), every route mounted during the scope is
// removed and any upstream connection acquired during it is released (closing
// it when it was the last reference). On success the routes stay and the
// connection reference is released when ctx gets done.
//
// Scopes opened from within an active scope (for example RegisterXHandler
// calling RegisterXHandlerClient) join the surrounding transaction.
//
// When the mux was configured with WithUpstreamConns and its connections have
// already been shut down, BeginRegistration fails with ErrUpstreamClosed.
func (s *ServeMux) BeginRegistration(ctx context.Context) (context.Context, func(*error), error) {
	if rs, ok := ctx.Value(registrationStateKey{}).(*registrationState); ok && rs.mux == s {
		rs.depth++
		return ctx, func(*error) { rs.depth-- }, nil
	}
	// Serialize transactions so the route snapshot and the rollback tail
	// computation cannot interleave with another registration's commits.
	s.regMu.Lock()
	if s.upstream != nil && s.upstream.isClosed() {
		s.regMu.Unlock()
		return ctx, nil, ErrUpstreamClosed
	}
	rs := &registrationState{
		mux:      s,
		ctx:      ctx,
		snapshot: s.snapshotRoutes(),
		depth:    1,
	}
	return context.WithValue(ctx, registrationStateKey{}, rs), rs.finish, nil
}

// finish is the outermost scope's commit/rollback hook.
func (rs *registrationState) finish(errp *error) {
	defer rs.mux.regMu.Unlock()
	if r := recover(); r != nil {
		rs.rollback()
		panic(r)
	}
	if errp != nil && *errp != nil {
		rs.rollback()
		return
	}
	rs.commit()
}

func (rs *registrationState) rollback() {
	rs.mux.rollbackRoutes(rs.snapshot)
	switch {
	case rs.mux.upstream != nil && rs.acquired != nil:
		// Reference-counted release: a failed registration never leaves a
		// dangling reference, and a shared connection survives as long as
		// another service still references it.
		rs.mux.upstream.release(rs.acquired)
	case rs.legacyConn != nil:
		if err := rs.legacyConn.Close(); err != nil {
			grpclog.Errorf("Failed to close conn to %s: %v", rs.legacyEndpoint, err)
		}
	}
}

func (rs *registrationState) commit() {
	switch {
	case rs.mux.upstream != nil && rs.acquired != nil:
		entry := rs.acquired
		go func() {
			<-rs.ctx.Done()
			rs.mux.upstream.release(entry)
		}()
	case rs.legacyConn != nil:
		conn := rs.legacyConn
		endpoint := rs.legacyEndpoint
		go func() {
			<-rs.ctx.Done()
			if err := conn.Close(); err != nil {
				grpclog.Errorf("Failed to close conn to %s: %v", endpoint, err)
			}
		}()
	}
}

func registrationStateFromContext(ctx context.Context) *registrationState {
	rs, _ := ctx.Value(registrationStateKey{}).(*registrationState)
	return rs
}

// RegisterFromEndpoint is the runtime counterpart of the generated
// Register<Service>HandlerFromEndpoint functions.
//
// It runs register inside a transactional registration after obtaining the
// upstream connection for (endpoint, opts). When the mux was configured with
// WithUpstreamConns, the connection is gateway-owned and shared per endpoint
// and dial options; otherwise the connection is dialed privately with exactly
// the historical grpc.NewClient call and closed when ctx gets done.
//
// If register fails, the routes it mounted are removed and the acquired
// connection is released/closed before the error is returned.
func (s *ServeMux) RegisterFromEndpoint(
	ctx context.Context,
	endpoint string,
	opts []grpc.DialOption,
	register func(context.Context, *grpc.ClientConn) error,
) (err error) {
	ctx, finish, err := s.BeginRegistration(ctx)
	if err != nil {
		return err
	}
	defer finish(&err)

	rs := registrationStateFromContext(ctx)
	if u := s.upstream; u != nil {
		entry, acquireErr := u.acquire(endpoint, opts)
		if acquireErr != nil {
			return acquireErr
		}
		rs.acquired = entry
		return register(ctx, entry.conn)
	}

	conn, dialErr := grpc.NewClient(endpoint, opts...)
	if dialErr != nil {
		return dialErr
	}
	rs.legacyConn = conn
	rs.legacyEndpoint = endpoint
	return register(ctx, conn)
}

// snapshotRoutes records the current number of handlers registered per HTTP
// method, so a failed registration can truncate back to this point.
func (s *ServeMux) snapshotRoutes() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := make(map[string]int, len(s.handlers))
	for method, handlers := range s.handlers {
		snapshot[method] = len(handlers)
	}
	return snapshot
}

// rollbackRoutes restores the handler slices captured in snapshot. Fresh
// backing arrays are used instead of in-place truncation, so requests already
// dispatching over the old slice never observe a concurrently modified array.
func (s *ServeMux) rollbackRoutes(snapshot map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Handle prepends new routes, so routes mounted during the failed
	// transaction sit at the front of each slice: keep the tail that existed
	// at snapshot time. Methods absent from the snapshot were created by the
	// failed transaction and are removed entirely.
	for method, current := range s.handlers {
		n := snapshot[method]
		if len(current) <= n {
			continue
		}
		if n == 0 {
			delete(s.handlers, method)
			continue
		}
		kept := make([]handler, n)
		copy(kept, current[len(current)-n:])
		s.handlers[method] = kept
	}
}
