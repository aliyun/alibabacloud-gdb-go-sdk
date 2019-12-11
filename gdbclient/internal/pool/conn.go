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
	"errors"
	"github.com/aliyun/alibabacloud-gdb-go-client/gdbclient/internal"
	"github.com/aliyun/alibabacloud-gdb-go-client/gdbclient/internal/graphsonv3"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var noDeadline = time.Time{}

type Conn interface {
	Close()
	UsedAt() time.Time
	CreatedAt() time.Time
	SetPooled(bool)
	Pooled() bool
	Connected() bool
	Disposed() bool

	SubmitRequestAsync(request *graphsonv3.Request) (*graphsonv3.ResponseFuture, error)
}

type ConnMock struct {
	createdAt time.Time
	usedAt    time.Time
	pooled    bool
	connected bool
	disposed  bool
}

func NewConnMock(opt *Options) (Conn, error) {
	return &ConnMock{
		createdAt: time.Now(),
		usedAt:    time.Now(),
		pooled:    false,
		connected: true,
		disposed:  false,
	}, nil
}

func (cn *ConnMock) Close() {
	cn.disposed = true
	cn.connected = false
}

func (cn *ConnMock) UsedAt() time.Time {
	return cn.usedAt
}

func (cn *ConnMock) CreatedAt() time.Time {
	return cn.createdAt
}

func (cn *ConnMock) SetPooled(pooled bool) {
	cn.pooled = pooled
}

func (cn *ConnMock) Pooled() bool {
	return cn.pooled
}

func (cn *ConnMock) Connected() bool {
	return cn.connected
}

func (cn *ConnMock) Disposed() bool {
	return cn.disposed
}

func (cn *ConnMock) SubmitRequestAsync(request *graphsonv3.Request) (*graphsonv3.ResponseFuture, error) {
	cn.usedAt = time.Now()
	return nil, errors.New("not support")
}

type ConnWebSocket struct {
	netConn          *websocket.Conn
	inChan           chan *graphsonv3.ResponseFuture
	PendingResponses *sync.Map

	pooled    bool
	createdAt time.Time
	usedAt    int64 // atomic

	quit chan struct{}
	wait *sync.WaitGroup

	opt *Options

	connected bool
	disposed  atomic.Value
	sync.RWMutex
}

func NewConnWebSocket(opt *Options) (Conn, error) {
	dialer := websocket.Dialer{
		WriteBufferSize:  1024 * 8,
		ReadBufferSize:   1024 * 8,
		HandshakeTimeout: 5 * time.Second,
	}

	url := "ws://" + opt.Endpoint + "/gremlin"
	netConn, _, err := dialer.Dial(url, http.Header{})
	if err != nil {
		return nil, err
	}

	cn := &ConnWebSocket{
		opt:              opt,
		netConn:          netConn,
		createdAt:        time.Now(),
		quit:             make(chan struct{}),
		wait:             &sync.WaitGroup{},
		PendingResponses: &sync.Map{},
		connected:        true,
		inChan:           make(chan *graphsonv3.ResponseFuture, opt.OnGoingRequests),
	}

	// alive check handler
	cn.netConn.SetPongHandler(
		func(appData string) error {
			cn.setConnected(true)
			return nil
		})

	cn.disposed.Store(false)
	cn.setUsedAt(time.Now())

	cn.wait.Add(3)
	// connect workers
	go cn.ping()
	go cn.writeWorker()
	go cn.readWorker()

	internal.Logger.Info("connect", zap.String("endpoint", opt.Endpoint),
		zap.Duration("pingInterval", opt.PingInterval),
		zap.Int("concurrent", opt.OnGoingRequests))
	return cn, nil
}

func (cn *ConnWebSocket) UsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

func (cn *ConnWebSocket) CreatedAt() time.Time {
	return cn.createdAt
}

func (cn *ConnWebSocket) Connected() bool {
	cn.RLock()
	defer cn.RUnlock()
	return cn.connected
}

func (cn *ConnWebSocket) Disposed() bool {
	return cn.disposed.Load().(bool)
}

func (cn *ConnWebSocket) Pooled() bool {
	return cn.pooled
}

func (cn *ConnWebSocket) SetPooled(pooled bool) {
	cn.pooled = pooled
}

func (cn *ConnWebSocket) setUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
}

func (cn *ConnWebSocket) setConnected(connect bool) {
	cn.Lock()
	defer cn.Unlock()
	cn.connected = connect
}

func (cn *ConnWebSocket) Close() {
	if cn.Disposed() {
		return
	}

	cn.disposed.Store(true)
	// close chan quit to wakeup all goroutine
	close(cn.quit)

	// send close message and close connect
	cn.netConn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	cn.netConn.Close()

	// wait all goroutine exit
	cn.wait.Wait()
	cn.setConnected(false)
	close(cn.inChan)

	// fill complete all pending response future
	cn.PendingResponses.Range(func(key, value interface{}) bool {
		response := graphsonv3.NewErrorResponse(key.(string),
			graphsonv3.RESPONSE_STATUS_REQUEST_ERROR_DELIVER,
			errors.New("GDB: connection closed"))
		value.(*graphsonv3.ResponseFuture).Complete(response)
		return true
	})
	cn.PendingResponses = &sync.Map{}

	internal.Logger.Info("connect close")
}

func (cn *ConnWebSocket) ping() {
	ticker := time.NewTicker(cn.opt.PingInterval)

	defer func() {
		ticker.Stop()
		cn.wait.Done()
	}()

	// ticker for ping
	for {
		if cn.Disposed() {
			internal.Logger.Info("ping Done as disposed")
			return
		}

		select {
		case <-ticker.C:
			connected := true
			err := cn.netConn.WriteControl(websocket.PingMessage, []byte{}, cn.deadline(cn.opt.WriteTimeout))
			if err != nil {
				internal.Logger.Error("Ping failed", zap.Error(err))
				connected = false
			}
			cn.setConnected(connected)
		case <-cn.quit:
			internal.Logger.Info("ping Done as quit")
			return
		}
	}
}

func (cn *ConnWebSocket) writeWorker() {
	defer cn.wait.Done()

	for {
		if cn.Disposed() {
			internal.Logger.Info("write worker exit due to disposed")
			return
		}

		select {
		case future, ok := <-cn.inChan:
			if !ok {
				continue
			}

			request := future.Request()

			// serializer request
			outBuf, err := graphsonv3.SerializerRequest(request)
			if err != nil {
				response := graphsonv3.NewErrorResponse(request.RequestID,
					graphsonv3.RESPONSE_STATUS_REQUEST_ERROR_SERIALIZATION, err)
				future.Complete(response)
				continue
			}

			// check pending or not
			if _, ok := cn.PendingResponses.LoadOrStore(request.RequestID, future); ok {
				// rewrite the same 'requestId' with pending requests
				if request.Op != internal.OPS_AUTHENTICATION {
					response := graphsonv3.NewErrorResponse(request.RequestID,
						graphsonv3.RESPONSE_STATUS_REQUEST_ERROR_DELIVER,
						errors.New("GDB: pending duplicate request id to server"))
					internal.Logger.Warn("request duplicate", zap.String("id ", request.RequestID))
					future.Complete(response)
					continue
				}
			}

			// send request to server
			if err = cn.netConn.SetWriteDeadline(cn.deadline(cn.opt.WriteTimeout)); err == nil {
				err = cn.netConn.WriteMessage(websocket.BinaryMessage, outBuf)
			}

			// check network write status and write back notifier to writer
			if err != nil {
				cn.PendingResponses.Delete(request.RequestID)

				response := graphsonv3.NewErrorResponse(request.RequestID,
					graphsonv3.RESPONSE_STATUS_REQUEST_ERROR_DELIVER, err)
				future.Complete(response)
				continue
			}
		case <-cn.quit:
			internal.Logger.Info("Write Done as quit")
			return
		}
	}
}

func (cn *ConnWebSocket) readWorker() {
	var errorTimes = 0
	defer cn.wait.Done()

	for {
		if cn.Disposed() {
			internal.Logger.Info("read worker exit due to disposed")
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
		cn.handleResponse(response)

		// check errors
		if err == nil {
			errorTimes = 0
		} else {
			errorTimes++
			if errorTimes > 10 {
				internal.Logger.Error("read worker exit due to error", zap.Error(err))
				return
			}
		}

		select {
		case <-cn.quit:
			internal.Logger.Info("Read Done as quit")
			return
		default:
			continue
		}
	}
}

func (cn *ConnWebSocket) handleResponse(response *graphsonv3.Response) {
	if response == nil {
		return
	}

	if response.Code == graphsonv3.RESPONSE_STATUS_AUTHENTICATE {
		request, _ := graphsonv3.MakeAuthRequest(response.RequestID, cn.opt.Username, cn.opt.Password)

		// block to queue auth request to server
		cn.inChan <- graphsonv3.NewResponseFuture(request)
		return
	}

	if future, ok := cn.PendingResponses.Load(response.RequestID); ok {
		responseFuture := future.(*graphsonv3.ResponseFuture)

		responseFuture.FixResponse(func(respChan *graphsonv3.Response) {
			respChan.Code = response.Code
			if respChan.Data == nil {
				respChan.Data = response.Data
			} else {
				// make Data as Slice when append
				if data, ok := respChan.Data.(json.RawMessage); ok {
					dataList := make([]json.RawMessage, 1, 8)
					dataList[0] = data
					respChan.Data = dataList
				}

				newData := response.Data.(json.RawMessage)
				dataList := respChan.Data.([]json.RawMessage)
				respChan.Data = append(dataList, newData)
			}
		})

		if response.Code != graphsonv3.RESPONSE_STATUS_PARITAL_CONTENT {
			// get a whole response, remove from pending queue then signal to
			cn.PendingResponses.Delete(response.RequestID)
			responseFuture.Complete(nil)

			if err, ok := response.Data.(error); ok {
				internal.Logger.Debug("response", zap.Int("code", response.Code), zap.Error(err))
			}
		}
	} else {
		internal.Logger.Error("handle response", zap.String("id", response.RequestID))
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
	if cn.Disposed() || !cn.Connected() {
		return nil, errors.New("connect not available")
	}

	responseFuture := graphsonv3.NewResponseFuture(request)
	// send request to out channel
	select {
	case cn.inChan <- responseFuture:
		break
	default:
		return nil, errors.New("GDB: output queue is full, overhead concurrent")
	}
	return responseFuture, nil
}
