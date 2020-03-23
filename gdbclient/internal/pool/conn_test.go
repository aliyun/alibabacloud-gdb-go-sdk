/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2020/2/28
 */

package pool

import (
	"errors"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/graph"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal/graphsonv3"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestNewConnWebSocket(t *testing.T) {
	server := StartGdbTestServer()
	defer server.CloseGdbTestServer()

	var _options = &Options{
		GdbUrl:              server.WsUrl,
		PingInterval:        2 * time.Second,
		WriteTimeout:        1 * time.Second,
		ReadTimeout:         1 * time.Second,
		MaxInProcessPerConn: 4,
	}

	Convey("connect and close", t, func() {
		conn, err := NewConnWebSocket(_options)
		So(conn, ShouldNotBeNil)
		So(err, ShouldBeNil)
		So(conn.closed(), ShouldBeFalse)
		So(conn.broken(), ShouldBeFalse)
		So(conn.pendingSize, ShouldEqual, 0)

		So(conn.createdAt.After(time.Now().Add(-1*time.Second)), ShouldBeTrue)

		conn.Close()
		So(conn.closed(), ShouldBeTrue)
	})

	Convey("send request and wait response", t, func() {
		conn, err := NewConnWebSocket(_options)
		So(conn, ShouldNotBeNil)
		So(err, ShouldBeNil)
		defer conn.Close()

		Convey("send one request and wait", func() {
			request, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", nil)
			respFuture, err := conn.SubmitRequestAsync(request)
			So(err, ShouldBeNil)
			So(conn.pendingSize, ShouldEqual, 1)

			resp := respFuture.Get()
			So(resp.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)
			So(resp.Data, ShouldNotBeNil)
		})

		Convey("send multi request async and wait", func() {
			request1, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", nil)
			respFuture1, err := conn.SubmitRequestAsync(request1)
			So(err, ShouldBeNil)

			request2, _ := graphsonv3.MakeRequestWithOptions("g.E().count()", nil)
			respFuture2, err := conn.SubmitRequestAsync(request2)
			So(err, ShouldBeNil)

			So(conn.pendingSize, ShouldEqual, 2)

			resp1 := respFuture1.Get()
			resp2 := respFuture2.Get()
			So(resp1.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)
			So(resp2.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)
			So(conn.pendingSize, ShouldEqual, 0)
		})

		Convey("send multi request and get one by one ", func() {
			request1, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", nil)
			respFuture1, err := conn.SubmitRequestAsync(request1)
			So(err, ShouldBeNil)

			resp1 := respFuture1.Get()
			So(conn.pendingSize, ShouldEqual, 0)

			request2, _ := graphsonv3.MakeRequestWithOptions("g.E().count()", nil)
			respFuture2, err := conn.SubmitRequestAsync(request2)
			So(err, ShouldBeNil)

			resp2 := respFuture2.Get()
			So(conn.pendingSize, ShouldEqual, 0)

			So(resp1.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)
			So(resp2.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)
		})

		Convey("send multi request async over max pending", func() {
			// make a delay during process on server
			orgFunc := server.WsMakeResponseFunc
			server.WsMakeResponseFunc = func(requestId string) []byte {
				time.Sleep(100 * time.Millisecond)
				return orgFunc(requestId)
			}

			for i := 0; i < _options.MaxInProcessPerConn; i++ {
				request, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", nil)
				respFuture, err := conn.SubmitRequestAsync(request)
				So(err, ShouldBeNil)
				So(respFuture.IsCompleted(), ShouldBeFalse)
			}
			So(conn.pendingSize, ShouldEqual, _options.MaxInProcessPerConn)

			request, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", nil)
			_, err := conn.SubmitRequestAsync(request)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, errOverQueue.Error())

			server.WsMakeResponseFunc = orgFunc
		})

		Convey("send request should be fail when connection close", func() {
			conn.Close()
			So(conn.closed(), ShouldBeTrue)

			request, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", nil)
			_, err := conn.SubmitRequestAsync(request)
			So(err, ShouldNotBeNil)
			So(conn.pendingSize, ShouldEqual, 0)

			So(err.Error(), ShouldEqual, errConnClosed.Error())
		})

		Convey("send options request", func() {
			opt := graph.NewRequestOptionsWithBindings(nil)
			id, err := uuid.NewUUID()
			So(err, ShouldBeNil)
			opt.SetRequestId(id.String())

			Convey("send requests with the same requestId", func() {
				request1, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", opt)
				respFuture1, err := conn.SubmitRequestAsync(request1)
				So(err, ShouldBeNil)
				resp1 := respFuture1.Get()

				So(resp1.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)
				So(resp1.RequestID, ShouldEqual, id.String())
				So(conn.pendingSize, ShouldEqual, 0)

				// send another request with the same requestId above
				request2, _ := graphsonv3.MakeRequestWithOptions("g.E().count()", opt)
				respFuture2, err := conn.SubmitRequestAsync(request2)
				So(err, ShouldBeNil)
				resp2 := respFuture2.Get()

				So(resp2.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)
				So(resp2.RequestID, ShouldEqual, id.String())
			})

			Convey("async send requests with the same requestId", func() {
				request1, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", opt)
				respFuture1, err := conn.SubmitRequestAsync(request1)
				So(err, ShouldBeNil)
				So(conn.pendingSize, ShouldEqual, 1)

				request2, _ := graphsonv3.MakeRequestWithOptions("g.E().count()", opt)
				respFuture2, err := conn.SubmitRequestAsync(request2)
				So(err, ShouldBeNil)

				So(respFuture2.IsCompleted(), ShouldBeTrue)
				So(respFuture2.Get().Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_REQUEST_ERROR_DELIVER)
				So(respFuture2.Get().Data, ShouldHaveSameTypeAs, errors.New(""))

				err = respFuture2.Get().Data.(error)
				So(err.Error(), ShouldEqual, errDuplicateId.Error())

				resp1 := respFuture1.Get()
				So(resp1.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)
				So(resp1.RequestID, ShouldEqual, id.String())
			})
		})
	})
}
