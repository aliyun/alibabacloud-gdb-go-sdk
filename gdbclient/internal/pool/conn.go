/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/11/25
 */

package pool

import (
	"encoding/json"
	"fmt"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/graph"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal/graphsonv3"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"math"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var noDeadline = time.Time{}

func zapPtr(conn *ConnWebSocket) zap.Field {
	return zap.Uintptr("conn", uintptr(unsafe.Pointer(conn)))
}

type ConnWebSocket struct {
	netConn          *websocket.Conn
	pendingResponses *sync.Map
	pendingSize      int32
	maxInProcess     int32

	createdAt time.Time
	usedAt    int64 // atomic
	borrowed  int32 // atomic

	opt         *Options
	notifier    pNotifier
	releaseConn pReleaseConn

	_broken bool
	_closed uint32 // atomic
	closeCn chan struct{}

	pingErrorsNum int
	wLock         sync.Mutex
}

func NewConnWebSocket(opt *Options) (*ConnWebSocket, error) {
	dialer := websocket.Dialer{
		WriteBufferSize:  1024 * 8,
		ReadBufferSize:   1024 * 8,
		HandshakeTimeout: 5 * time.Second,
	}

	netConn, _, err := dialer.Dial(opt.GdbUrl, http.Header{})
	if err != nil {
		return nil, err
	}

	cn := &ConnWebSocket{
		opt:              opt,
		netConn:          netConn,
		createdAt:        time.Now(),
		closeCn:          make(chan struct{}),
		pendingResponses: &sync.Map{},
		maxInProcess:     int32(opt.MaxInProcessPerConn),
	}

	cn.setUsedAt(time.Now())

	// connect workers
	if opt.PingInterval > 0 {
		go cn.connCheck(opt.PingInterval)
	}
	go cn.readResponse()

	internal.Logger.Info("create connect", zap.String("url", opt.GdbUrl),
		zap.Int("concurrent", opt.MaxInProcessPerConn), zapPtr(cn), zap.Duration("pingInterval", opt.PingInterval))
	return cn, nil
}

func (cn *ConnWebSocket) String() string {
	return fmt.Sprintf("conn<%d>: createAt %s, usedAt %s, borrowed %d, pending %d,"+
		" broken %t, closed %t, pingErrorNum %d",
		uintptr(unsafe.Pointer(cn)), cn.createdAt.Format("2006-01-02_3:04:05.000"),
		cn.UsedAt().Format("2006-01-02_3:04:05.000"),
		atomic.LoadInt32(&cn.borrowed), atomic.LoadInt32(&cn.pendingSize),
		cn._broken, cn.closed(), cn.pingErrorsNum)
}

func (cn *ConnWebSocket) Close() {
	if !atomic.CompareAndSwapUint32(&cn._closed, 0, 1) {
		return
	}

	// close chan quit to wakeup all goroutine
	close(cn.closeCn)

	// close connection
	cn.netConn.Close()

	// fill complete all pending response future
	cn.pendingResponses.Range(func(key, value interface{}) bool {
		response := graphsonv3.NewErrorResponse(key.(string),
			graphsonv3.RESPONSE_STATUS_REQUEST_ERROR_DELIVER, errConnClosed)
		value.(*graphsonv3.ResponseFuture).Complete(response)
		return true
	})
	atomic.StoreInt32(&cn.pendingSize, 0)
	cn.pendingResponses = &sync.Map{}
	internal.Logger.Info("connect close", zapPtr(cn))
}

func (cn *ConnWebSocket) UsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

func (cn *ConnWebSocket) CreatedAt() time.Time {
	return cn.createdAt
}

func (cn *ConnWebSocket) setUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
}

func (cn *ConnWebSocket) setNotifier(n pNotifier) {
	cn.notifier = n
}

func (cn *ConnWebSocket) setReleaseConn(n pReleaseConn) {
	cn.releaseConn = n
}

func (cn *ConnWebSocket) returnToPool() bool {
	if cn.releaseConn != nil {
		cn.releaseConn(cn)
	}
	return true
}

func (cn *ConnWebSocket) connCheck(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// It is possible that ticker and closedCn arrive together,
			// and select pseudo-randomly pick ticker case, we double
			// check here to prevent being executed after closed.
			if cn.closed() {
				return
			}
			err := cn.doping(3)
			if err != nil {
				cn.pingErrorsNum += 1
				internal.Logger.Error("status check", zapPtr(cn), zap.Time("time", time.Now()), zap.Error(err))
				if cn.pingErrorsNum >= 3 {
					cn._broken = true
					// wakeup pool to check connection status
					_ = cn.notifier != nil && cn.notifier()
					internal.Logger.Error("conn ping broken", zapPtr(cn), zap.Time("time", time.Now()))
					return
				}
			} else {
				cn.pingErrorsNum = 0
			}
		case <-cn.closeCn:
			return
		}
	}
}

func (cn *ConnWebSocket) doping(retry int) error {
	var err error
	for i := 0; i < retry && !cn.brokenOrClosed(); i++ {
		err = cn.netConn.WriteControl(websocket.PingMessage, []byte{}, cn.deadline(cn.opt.WriteTimeout))
		if err == nil {
			return nil
		}
		internal.Logger.Debug("ping failed", zapPtr(cn), zap.Time("time", time.Now()), zap.Error(err))
		time.Sleep(time.Second)
	}
	return err
}

func (cn *ConnWebSocket) broken() bool {
	return cn._broken
}

func (cn *ConnWebSocket) closed() bool {
	return atomic.LoadUint32(&cn._closed) == 1
}

func (cn *ConnWebSocket) brokenOrClosed() bool {
	return cn._broken || cn.closed()
}

func (cn *ConnWebSocket) availableInProcess() int32 {
	return int32(math.Max(0, float64(cn.maxInProcess-atomic.LoadInt32(&cn.pendingSize))))
}

func (cn *ConnWebSocket) readResponse() {
	var errorTimes = 0

	for {
		if cn.brokenOrClosed() {
			internal.Logger.Info("conn read routine exit", zapPtr(cn), zap.Time("time", time.Now()))
			return
		}

		var msg []byte
		var err error
		var response *graphsonv3.Response

		// read response as block, exit by io close signal
		if err = cn.netConn.SetReadDeadline(cn.deadline(0)); err == nil {
			if _, msg, err = cn.netConn.ReadMessage(); err == nil {
				response, err = graphsonv3.ReadResponse(msg)
			}
		}
		// handle response and tick future
		if response != nil {
			cn.handleResponse(response)
		}

		// check errors
		if err != nil {
			errorTimes++
			if errorTimes > 10 {
				cn._broken = true
				_ = cn.notifier != nil && cn.notifier()
				internal.Logger.Error("conn read broken", zapPtr(cn),zap.Time("time", time.Now()), zap.Error(err))
				return
			}
		} else {
			errorTimes = 0
		}
	}
}

func (cn *ConnWebSocket) handleResponse(response *graphsonv3.Response) {
	if response.Code == graphsonv3.RESPONSE_STATUS_AUTHENTICATE {
		request, _ := graphsonv3.MakeAuthRequest(response.RequestID, cn.opt.Username, cn.opt.Password)

		// append auth request to server and do not care return future
		cn.SubmitRequestAsync(request)
		return
	}

	if future, ok := cn.pendingResponses.Load(response.RequestID); ok {
		responseFuture := future.(*graphsonv3.ResponseFuture)

		responseFuture.FixResponse(func(respChan *graphsonv3.Response) {
			respChan.Code = response.Code
			if respChan.Data == nil {
				respChan.Data = response.Data
			} else {
				if newData, ok := response.Data.(json.RawMessage); ok {
					// make Data as Slice when json.RawMessage append
					if data, ok := respChan.Data.(json.RawMessage); ok {
						dataList := make([]json.RawMessage, 2, 8)
						dataList[0] = data
						dataList[1] = newData
						respChan.Data = dataList
					} else if dataList, ok := respChan.Data.([]json.RawMessage); ok {
						respChan.Data = append(dataList, newData)
					} else {
						// FIXME: incoming rawMessage but couldn't append to
						internal.Logger.Error("incoming rawMessage after", zap.Time("time", time.Now()), zap.Stringer("data", reflect.TypeOf(respChan.Data)))
					}
				} else if newData, ok := response.Data.(error); ok {
					// FIXME: incoming a error, ignore it if here is before, take it if not
					if _, isErr := respChan.Data.(error); !isErr {
						respChan.Data = newData
					}
					internal.Logger.Debug("incoming error after", zap.Time("time", time.Now()), zap.Stringer("data", reflect.TypeOf(respChan.Data)))
				} else {
					internal.Logger.Error("ignore incoming message", zap.Time("time", time.Now()), zap.Stringer("data", reflect.TypeOf(response.Data)))
				}
			}
		})

		if response.Code != graphsonv3.RESPONSE_STATUS_PARITAL_CONTENT {
			// get a whole response, remove from pending queue then signal to
			cn.pendingResponses.Delete(response.RequestID)
			atomic.AddInt32(&cn.pendingSize, -1)
			responseFuture.Complete(nil)

			if (response.Code != graphsonv3.RESPONSE_STATUS_SUCCESS) && (response.Code != graphsonv3.RESPONSE_STATUS_NO_CONTENT) {
				internal.Logger.Debug("response", zap.Time("time", time.Now()), zap.Int("code", response.Code),
					zap.String("error", fmt.Sprint(response.Data)))
			}
		}
	} else {
		internal.Logger.Error("handle response not found", zap.Time("time", time.Now()), zap.String("id", response.RequestID))
	}
}

func (cn *ConnWebSocket) deadline(timeout time.Duration) time.Time {
	tm := time.Now()
	cn.setUsedAt(tm)

	if timeout > 0 {
		return tm.Add(timeout)
	}
	return noDeadline
}

func (cn *ConnWebSocket) SubmitRequestAsync(request *graphsonv3.Request) (*graphsonv3.ResponseFuture, error) {
	if cn.brokenOrClosed() {
		internal.Logger.Error("request send close", zapPtr(cn), zap.Time("time", time.Now()), zap.Error(errConnClosed))
		return nil, errConnClosed
	}
	if atomic.LoadInt32(&cn.pendingSize) >= cn.maxInProcess {
		internal.Logger.Error("conn", zap.Stringer("cn", cn))
		internal.Logger.Error("request send over", zapPtr(cn), zap.Time("time", time.Now()), zap.Error(errOverQueue))
		return nil, errOverQueue
	}

	future := graphsonv3.NewResponseFuture(request, cn.returnToPool)
	// serializer request
	outBuf, err := graphsonv3.SerializerRequest(request)
	if err != nil {
		response := graphsonv3.NewErrorResponse(request.RequestID,
			graphsonv3.RESPONSE_STATUS_REQUEST_ERROR_SERIALIZATION, err)
		future.Complete(response)
		internal.Logger.Error("request send serializer", zapPtr(cn), zap.Time("time", time.Now()), zap.Error(err))
		return future, nil
	}

	// check pending or not
	if _, ok := cn.pendingResponses.LoadOrStore(request.RequestID, future); ok {
		// rewrite the same 'requestId' with pending requests
		if request.Op != graph.OPS_AUTHENTICATION {
			response := graphsonv3.NewErrorResponse(request.RequestID,
				graphsonv3.RESPONSE_STATUS_REQUEST_ERROR_DELIVER,
				errDuplicateId)
			internal.Logger.Error("request duplicate", zap.Time("time", time.Now()), zap.String("id ", request.RequestID))
			future.Complete(response)
			return future, nil
		}
	} else {
		atomic.AddInt32(&cn.pendingSize, 1)
	}

	// send request to server
	cn.wLock.Lock()
	if err = cn.netConn.SetWriteDeadline(cn.deadline(cn.opt.WriteTimeout)); err == nil {
		err = cn.netConn.WriteMessage(websocket.BinaryMessage, outBuf)
	}
	cn.wLock.Unlock()

	// check network write status and write back notifier to writer
	if err != nil {
		response := graphsonv3.NewErrorResponse(request.RequestID,
			graphsonv3.RESPONSE_STATUS_REQUEST_ERROR_DELIVER, err)

		cn.pendingResponses.Delete(request.RequestID)
		atomic.AddInt32(&cn.pendingSize, -1)

		future.Complete(response)
		internal.Logger.Error("request send io", zapPtr(cn), zap.Time("time", time.Now()), zap.Error(err))
	}
	return future, nil
}
