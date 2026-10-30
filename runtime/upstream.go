package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

// ErrUpstreamClosed is returned by registration calls on a ServeMux whose
// gateway-owned upstream connections have already been shut down.
//
// Use errors.Is to distinguish it from ordinary registration failures and from
// ErrUpstreamDrainTimeout.
var ErrUpstreamClosed = errors.New("grpc-gateway: upstream connection manager is closed")

// ErrUpstreamDrainTimeout is returned by UpstreamConns.Shutdown when in-flight
// requests did not finish within the declared drain window. In that case all
// upstream connections are still closed forcefully, so shutdown always reaches
// a deterministic end state.
//
// Use errors.Is to distinguish it from ordinary registration failures and from
// ErrUpstreamClosed.
var ErrUpstreamDrainTimeout = errors.New("grpc-gateway: timed out draining in-flight requests before closing upstream connections")

// UpstreamConns owns gRPC client connections on behalf of a ServeMux and shares
// them between services.
//
// A single connection is established for each unique pair of upstream endpoint
// and dial options; every registration to the same endpoint with the same
// group of dial options reuses it. Each successful registration holds one
// reference which is released when the context passed to registration is done
// (matching the historical per-registration connection lifetime). The
// underlying connection is only closed once its last reference is released.
//
// Without attaching an UpstreamConns to a ServeMux (see WithUpstreamConns) the
// gateway keeps its historical behavior: every FromEndpoint registration dials
// and closes its own connection independently.
//
// UpstreamConns does not perform name resolution, load balancing or proxying
// itself; it merely owns *grpc.ClientConn values created with exactly the dial
// options provided by the caller.
type UpstreamConns struct {
	mu       sync.Mutex
	entries  map[string][]*upstreamConn
	shutting bool
	closed   bool
	// done is closed once Shutdown has finished closing all connections.
	done chan struct{}

	// inflight tracks requests admitted while the manager was open. All Add
	// calls happen under mu and stop being admitted once shutting is set, so
	// Shutdown can safely wait on the group.
	inflight sync.WaitGroup

	dials  atomic.Int64
	closes atomic.Int64
}

// NewUpstreamConns returns a ready-to-use manager of gateway-owned upstream
// connections. Attach it to a ServeMux with WithUpstreamConns, and call
// Shutdown when the gateway is taken out of service.
func NewUpstreamConns() *UpstreamConns {
	return &UpstreamConns{
		entries: make(map[string][]*upstreamConn),
		done:    make(chan struct{}),
	}
}

// UpstreamConnsStats reports observable counters of an UpstreamConns.
type UpstreamConnsStats struct {
	// Dials is the total number of upstream connections established
	// (grpc.NewClient calls performed by the gateway).
	Dials int64
	// Closes is the total number of upstream connections closed, either
	// because the last reference was released or because of Shutdown.
	Closes int64
	// ActiveConnections is the number of connections currently held open.
	ActiveConnections int
	// References is the total number of live registration references across
	// all active connections.
	References int
}

// Stats returns a point-in-time snapshot of the connection counters. The
// counters are safe to read while registrations and requests are in flight.
func (u *UpstreamConns) Stats() UpstreamConnsStats {
	u.mu.Lock()
	defer u.mu.Unlock()
	stats := UpstreamConnsStats{
		Dials:  u.dials.Load(),
		Closes: u.closes.Load(),
	}
	for _, list := range u.entries {
		for _, e := range list {
			if e.removed {
				continue
			}
			stats.ActiveConnections++
			stats.References += e.refs
		}
	}
	return stats
}

// acquire returns the shared connection for (endpoint, opts), establishing it
// on first use, and records one registration reference. It fails with
// ErrUpstreamClosed once Shutdown has started; dial errors are returned
// unwrapped in origin (wrapped only with endpoint context) so callers can
// attribute them as ordinary registration failures.
func (u *UpstreamConns) acquire(endpoint string, opts []grpc.DialOption) (*upstreamConn, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.shutting || u.closed {
		return nil, ErrUpstreamClosed
	}
	for _, e := range u.entries[endpoint] {
		if !e.removed && dialOptionsEqual(e.opts, opts) {
			e.refs++
			return e, nil
		}
	}
	// grpc.NewClient is lazy and non-blocking; holding the manager lock while
	// calling it keeps first-use serialization simple.
	conn, err := grpc.NewClient(endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc-gateway: dial upstream %q: %w", endpoint, err)
	}
	e := &upstreamConn{
		endpoint: endpoint,
		opts:     opts,
		conn:     conn,
		refs:     1,
	}
	u.entries[endpoint] = append(u.entries[endpoint], e)
	u.dials.Add(1)
	return e, nil
}

// release drops one registration reference. When the last reference for a
// connection goes away the connection is removed from the shared pool and
// closed. Releasing more times than there were successful acquires, or after
// Shutdown, is a no-op: repeated release is idempotent.
func (u *UpstreamConns) release(e *upstreamConn) {
	u.mu.Lock()
	if e.removed {
		u.mu.Unlock()
		return
	}
	e.refs--
	if e.refs > 0 {
		u.mu.Unlock()
		return
	}
	u.removeEntryLocked(e)
	u.mu.Unlock()
	u.closeConn(e)
}

// Shutdown marks the manager closed, drains requests that were in flight at
// that point for as long as ctx allows, and then closes every pooled
// connection, regardless of how many references remain.
//
// The deadline (or cancellation) of ctx declares the drain window: requests
// completing before it finishes keep using their open connection (no
// use-after-close window), and when it elapses all connections are closed
// forcefully and ErrUpstreamDrainTimeout is returned. Registrations racing
// with Shutdown fail with ErrUpstreamClosed.
//
// Calling Shutdown more than once is idempotent and returns nil.
func (u *UpstreamConns) Shutdown(ctx context.Context) error {
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return nil
	}
	if u.shutting {
		u.mu.Unlock()
		return u.waitClosed(ctx)
	}
	u.shutting = true
	var toClose []*upstreamConn
	for endpoint, list := range u.entries {
		for _, e := range list {
			if !e.removed {
				e.removed = true
				toClose = append(toClose, e)
			}
		}
		delete(u.entries, endpoint)
	}
	u.mu.Unlock()

	drained := make(chan struct{})
	go func() {
		u.inflight.Wait()
		close(drained)
	}()
	var drainErr error
	select {
	case <-drained:
	case <-ctx.Done():
		drainErr = ErrUpstreamDrainTimeout
	}

	closeErrs := make([]error, 0, len(toClose)+1)
	if drainErr != nil {
		closeErrs = append(closeErrs, drainErr)
	}
	for _, e := range toClose {
		u.closes.Add(1)
		if err := e.conn.Close(); err != nil {
			closeErrs = append(closeErrs, fmt.Errorf("grpc-gateway: close upstream %q: %w", e.endpoint, err))
		}
	}

	u.mu.Lock()
	u.closed = true
	close(u.done)
	u.mu.Unlock()
	return errors.Join(closeErrs...)
}

// waitClosed blocks until a Shutdown in progress reaches its end state, or
// until ctx is done. It is used by concurrent Shutdown callers.
func (u *UpstreamConns) waitClosed(ctx context.Context) error {
	select {
	case <-u.done:
		return nil
	case <-ctx.Done():
		return ErrUpstreamDrainTimeout
	}
}

func (u *UpstreamConns) isClosed() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.shutting || u.closed
}

// beginRequest admits one in-flight request for drain accounting. Requests
// arriving after Shutdown started are not counted; their RPCs are terminated
// deterministically when the connections are force-closed.
func (u *UpstreamConns) beginRequest() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.shutting || u.closed {
		return false
	}
	u.inflight.Add(1)
	return true
}

func (u *UpstreamConns) endRequest(admitted bool) {
	if admitted {
		u.inflight.Done()
	}
}

func (u *UpstreamConns) removeEntryLocked(target *upstreamConn) {
	list := u.entries[target.endpoint]
	kept := list[:0]
	for _, e := range list {
		if e != target {
			kept = append(kept, e)
		}
	}
	if len(kept) == 0 {
		delete(u.entries, target.endpoint)
	} else {
		u.entries[target.endpoint] = kept
	}
	target.removed = true
}

func (u *UpstreamConns) closeConn(e *upstreamConn) {
	u.closes.Add(1)
	if err := e.conn.Close(); err != nil {
		grpclog.Errorf("Failed to close conn to %s: %v", e.endpoint, err)
	}
}

// upstreamConn is one gateway-owned shared gRPC client connection.
type upstreamConn struct {
	endpoint string
	opts     []grpc.DialOption
	conn     *grpc.ClientConn
	refs     int
	// removed means the entry has been taken out of the pool map, either
	// because its last reference was released or because Shutdown ran.
	removed bool
}

// dialOptionsEqual reports whether two groups of dial options are equivalent.
// Dial options produced by the grpc package carry closures, so two option
// groups are considered equal when they contain the same options in the same
// order (typically by passing the same opts slice to every FromEndpoint
// registration). Separate but visually identical option values are not shared.
func dialOptionsEqual(a, b []grpc.DialOption) bool {
	return reflect.DeepEqual(a, b)
}
