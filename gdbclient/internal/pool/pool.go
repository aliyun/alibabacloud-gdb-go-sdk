/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/11/29
 */

package pool

import (
	"errors"
	"fmt"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal"
	"go.uber.org/zap"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errConnClosed     = errors.New("GDB: connection closed")
	errOverQueue      = errors.New("GDB: request queue is full, overhead concurrent")
	errDuplicateId    = errors.New("GDB: pending duplicate request id to server")
	errGetConnTimeout = errors.New("GDB: get connection timeout")
	errPoolClosed     = errors.New("GDB: connection pool closed")
)

type Options struct {
	Dialer   func(*Options) (*ConnWebSocket, error)
	GdbUrl   string
	Username string
	Password string

	PoolSize           int
	PoolTimeout        time.Duration
	AliveCheckInterval time.Duration

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PingInterval time.Duration

	MaxInProcessPerConn         int
	MaxSimultaneousUsagePerConn int
}

type pNotifier func() bool
type pReleaseConn func(socket *ConnWebSocket)

type ConnPool struct {
	opt *Options

	dialErrorsNum   uint32 // atomic
	lastDialErrorMu sync.RWMutex
	lastDialError   error

	connsMu                     sync.RWMutex
	conns                       []*ConnWebSocket
	poolSize                    int
	hasAvailableConn            chan struct{}
	maxSimultaneousUsagePerConn int

	_closed  uint32 // atomic
	_opening int32  // atomic
	closedCh chan struct{}
	checkCh  chan struct{}
}

func NewConnPool(opt *Options) *ConnPool {
	if os.Getenv("GO_CLIENT_TEST_URL") != "" {
		opt.GdbUrl = os.Getenv("GO_CLIENT_TEST_URL")
		internal.Logger.Info("GDB CLIENT IN TEST MODE")
	}

	p := &ConnPool{
		opt:      opt,
		conns:    make([]*ConnWebSocket, 0, opt.PoolSize),
		poolSize: opt.PoolSize,

		_closed:          0,
		_opening:         0,
		closedCh:         make(chan struct{}),
		checkCh:          make(chan struct{}),
		hasAvailableConn: make(chan struct{}),

		maxSimultaneousUsagePerConn: opt.MaxSimultaneousUsagePerConn,
	}

	p.addConns()
	if opt.AliveCheckInterval > 0 {
		go p.checker(p.opt.AliveCheckInterval)
	}

	internal.Logger.Info("create pool", zap.Int("size", p.poolSize),
		zap.Duration("get timeout", opt.PoolTimeout), zap.Duration("alive freq", opt.AliveCheckInterval))
	return p
}

func (p *ConnPool) addConns() {
	if atomic.LoadInt32(&p._opening) > 0 || p.closed() {
		internal.Logger.Debug("pool is opening or closed")
		return
	}

	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.poolSize) {
		internal.Logger.Debug("dial con over number")
		return
	}

	internal.Logger.Debug("new conn async", zap.Int("current", p.Size()), zap.Int("target", p.poolSize))
	for i := p.Size(); i < p.poolSize; i++ {
		go p.newConn()
	}
}

func (p *ConnPool) newConn() {
	defer atomic.AddInt32(&p._opening, -1)
	if atomic.AddInt32(&p._opening, 1) > int32(p.poolSize) {
		return
	}

	cn, err := p.dialConn()
	if err != nil {
		internal.Logger.Error("dialer connect", zap.Error(err))
		return
	}

	p.connsMu.Lock()
	if !p.closed() && len(p.conns) <= p.poolSize {
		cn.setNotifier(p.poolNotifier)
		cn.setReleaseConn(p.Put)
		p.conns = append(p.conns, cn)
	}
	cn = nil
	p.connsMu.Unlock()

	if cn != nil {
		internal.Logger.Debug("release conn as pool full", zap.Stringer("con", cn))
		cn.Close()
	} else {
		p.announceAvailableConn()
	}
}

func (p *ConnPool) dialConn() (*ConnWebSocket, error) {
	if p.closed() {
		return nil, errPoolClosed
	}

	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}

	cn, err := p.opt.Dialer(p.opt)
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}
	return cn, nil
}

func (p *ConnPool) tryDial() {
	for {
		if p.closed() {
			internal.Logger.Debug("try routine gone as pool closed")
			return
		}

		conn, err := p.opt.Dialer(p.opt)
		if err != nil {
			internal.Logger.Info("try dial conn", zap.String("host", p.opt.GdbUrl), zap.Error(err))
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		internal.Logger.Info("try to dial server success")
		atomic.StoreUint32(&p.dialErrorsNum, 0)
		conn.Close()

		// add conn to pool as connection recover
		p.addConns()
		return
	}
}

func (p *ConnPool) Get() (*ConnWebSocket, error) {
	if p.closed() {
		return nil, errPoolClosed
	}
	return p.borrowConn(p.opt.PoolTimeout)
}

func (p *ConnPool) Put(cn *ConnWebSocket) {
	if p.closed() {
		internal.Logger.Error("put conn", zap.Error(errPoolClosed))
		return
	}
	p.returnConn(cn)
}

// Size returns total number of connections.
func (p *ConnPool) Size() int {
	p.connsMu.RLock()
	n := len(p.conns)
	p.connsMu.RUnlock()
	return n
}

// close connection pool
func (p *ConnPool) Close() {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return
	}
	internal.Logger.Info("close pool", zap.Int("size", p.Size()))
	close(p.closedCh)

	p.connsMu.Lock()
	for _, cn := range p.conns {
		p.closeConn(cn)
	}
	p.conns = nil
	p.connsMu.Unlock()
}

func (p *ConnPool) String() string {
	var consStrs []string
	p.connsMu.RLock()
	for _, cn := range p.conns {
		consStrs = append(consStrs, "{"+cn.String()+"}")
	}
	connLen := len(p.conns)
	p.connsMu.RUnlock()

	errorStr := "{}"
	if atomic.LoadUint32(&p.dialErrorsNum) > 0 {
		errorStr = fmt.Sprintf("{errNum: %d, errStr: %s}", p.dialErrorsNum, p.getLastDialError().Error())
	}
	return fmt.Sprintf("pool<%p> size %d, opening %d, closed %t, errors: %s, conns: [%s]",
		p, connLen, p._opening, p.closed(), errorStr, strings.Join(consStrs, ","))
}

func (p *ConnPool) setLastDialError(err error) {
	p.lastDialErrorMu.Lock()
	p.lastDialError = err
	p.lastDialErrorMu.Unlock()
}

func (p *ConnPool) getLastDialError() error {
	p.lastDialErrorMu.RLock()
	err := p.lastDialError
	p.lastDialErrorMu.RUnlock()
	return err
}

func (p *ConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

func (p *ConnPool) awaitAvailableConn(timeout time.Duration) bool {
	select {
	case <-time.After(timeout):
		return false
	case <-p.hasAvailableConn:
		return true
	}
}

func (p *ConnPool) announceAvailableConn() {
	select {
	case p.hasAvailableConn <- struct{}{}:
	default:
	}
}

func (p *ConnPool) removeConn(cn *ConnWebSocket) {
	p.connsMu.Lock()
	for i, c := range p.conns {
		if c == cn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}
	p.connsMu.Unlock()
}

func (p *ConnPool) returnConn(conn *ConnWebSocket) {
	atomic.AddInt32(&conn.borrowed, -1)

	internal.Logger.Debug("return conn", zapPtr(conn))
	if conn.brokenOrClosed() {
		internal.Logger.Debug("return broken conn", zap.Stringer("cn", conn))
		p.removeConn(conn)
		conn.Close()

		// active to dial a new connection to replace this conn
		p.addConns()
	} else {
		p.announceAvailableConn()
	}
}

func (p *ConnPool) borrowConn(timeout time.Duration) (*ConnWebSocket, error) {
	conn := p.selectLeastUsed()
	if conn == nil {
		internal.Logger.Debug("borrow conn nil", zap.Int("poolSize", p.Size()))
		return p.waitForConn(timeout)
	}

	for {
		inFlight := atomic.LoadInt32(&conn.borrowed)
		available := conn.availableInProcess()
		if inFlight >= int32(p.maxSimultaneousUsagePerConn) && available == 0 {
			internal.Logger.Debug("wait conn", zapPtr(conn),
				zap.Int32("flight", conn.borrowed), zap.Int32("availableInProcess", available))
			return p.waitForConn(timeout)
		}
		if atomic.CompareAndSwapInt32(&conn.borrowed, inFlight, inFlight+1) {
			internal.Logger.Debug("borrowed conn", zapPtr(conn),
				zap.Int32("flight", conn.borrowed), zap.Int32("availableInProcess", available))
			return conn, nil
		}
	}
}

func (p *ConnPool) waitForConn(timeout time.Duration) (*ConnWebSocket, error) {
	endtime := time.Now().Add(timeout)

	for remaining := timeout; remaining > 0; remaining = endtime.Sub(time.Now()) {
		internal.Logger.Debug("wait conn", zap.Time("now", time.Now()), zap.Duration("timeout", remaining))
		ok := p.awaitAvailableConn(remaining)
		if !ok {
			internal.Logger.Debug("wait conn timeout")
			return nil, errGetConnTimeout
		}
		if p.closed() {
			internal.Logger.Debug("wait conn failed as pool closed")
			return nil, errPoolClosed
		}

		conn := p.selectLeastUsed()
		for conn != nil {
			inFlight := atomic.LoadInt32(&conn.borrowed)
			available := conn.availableInProcess()
			// FIXME: connection available
			// break to wait again if inFlight >= available in Java SDK
			// why do set to wait again if connection available, so typo it now
			if available == 0 {
				internal.Logger.Info("wait conn may timeout", zapPtr(conn),
					zap.Int32("inFlight", inFlight), zap.Int32("availableInProcess", available))
				break
			}
			if atomic.CompareAndSwapInt32(&conn.borrowed, inFlight, inFlight+1) {
				return conn, nil
			}
		}
	}

	return nil, errGetConnTimeout
}

func (p *ConnPool) selectLeastUsed() *ConnWebSocket {
	minInFlight := int32(math.MaxInt32)
	var leastBusy *ConnWebSocket
	p.connsMu.RLock()
	for _, cn := range p.conns {
		inFlight := atomic.LoadInt32(&cn.borrowed)
		if !cn.brokenOrClosed() && inFlight < minInFlight {
			minInFlight = inFlight
			leastBusy = cn
		}
	}
	p.connsMu.RUnlock()
	return leastBusy
}

func (p *ConnPool) checker(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()
	var mFreq uint64 = 0

	for {
		select {
		case <-ticker.C:
			p.doCheck()
			// print pool status to info log
			if mFreq%5 == 0 {
				internal.Logger.Info("status", zap.Stringer("pool", p))
			}
			mFreq++
		case <-p.checkCh:
			p.doCheck()
		case <-p.closedCh:
			return
		}
	}
}

func (p *ConnPool) poolNotifier() bool {
	if p.closed() {
		return false
	}

	select {
	case p.checkCh <- struct{}{}:
		return true
	default:
		return false
	}
}

func (p *ConnPool) doCheck() {
	// It is possible that ticker and closedCh arrive together,
	// and select pseudo-randomly pick ticker case, we double
	// check here to prevent being executed after closed.
	if p.closed() {
		return
	}
	count := p.reapStaleConns()
	if count > 0 {
		internal.Logger.Debug("reaper stale conns", zap.Int("count", count))
		p.addConns()
	}
}

func (p *ConnPool) reapStaleConns() int {
	brokenConns := make([]*ConnWebSocket, 0)

	p.connsMu.Lock()
restart:
	for i, cn := range p.conns {
		if cn.brokenOrClosed() {
			brokenConns = append(brokenConns, cn)
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			goto restart
		}
	}
	p.connsMu.Unlock()

	for _, cn := range brokenConns {
		internal.Logger.Debug("reap broken conn", zap.Stringer("str", cn))
		p.closeConn(cn)
	}
	return len(brokenConns)
}

func (p *ConnPool) closeConn(cn *ConnWebSocket) {
	cn.Close()
}
