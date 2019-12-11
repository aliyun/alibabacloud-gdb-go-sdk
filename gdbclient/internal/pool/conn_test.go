/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/11/27
 */

package pool

import (
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal/graphsonv3"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestNewConnConn(t *testing.T) {
	//options := &Options{
	//	Endpoint:        "ws://127.0.0.1:8182/gremlin",
	//	Username:        "zhiyan",
	//	Password:        "zhiyan",
	//	PingInterval:    2 * time.Second,
	//	WriteTimeout:    1 * time.Second,
	//	ReadTimeout:     1 * time.Second,
	//	OnGoingRequests: 16,
	//}

	Convey("connect and close", t, func() {
		options := &Options{
			PingInterval: 2 * time.Second,
			WriteTimeout: 1 * time.Second,
			ReadTimeout:  1 * time.Second,
		}

		conn, err := NewConnMock(options)

		So(conn, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(conn.Connected(), ShouldBeTrue)
		So(conn.Disposed(), ShouldBeFalse)

		conn.Close()

		So(conn.CreatedAt().After(time.Now().Add(-1*time.Second)), ShouldBeTrue)
		So(conn.Connected(), ShouldBeFalse)
		So(conn.Disposed(), ShouldBeTrue)
	})

	//Convey("connect and send dsl", t, func() {
	SkipConvey("connect and send dsl", t, func() {
		options := &Options{
			Endpoint:        "127.0.0.1:8182",
			Username:        "zhiyan",
			Password:        "zhiyan",
			PingInterval:    2 * time.Second,
			WriteTimeout:    1 * time.Second,
			ReadTimeout:     1 * time.Second,
			OnGoingRequests: 16,
		}

		Convey("connect client", func() {
			conn, err := NewConnWebSocket(options)
			So(conn, ShouldNotBeNil)
			So(err, ShouldBeNil)

			Convey("send dsl and wait response", func() {
				request, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", nil)

				respFuture, err := conn.SubmitRequestAsync(request)
				So(err, ShouldBeNil)

				resp := respFuture.Get()
				So(resp.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)
				So(resp.Data, ShouldNotBeNil)
			})

			Convey("send dsl and no response content", func() {
				bindings := make(map[string]interface{})
				bindings["GDB___id"] = "___test_go_id_test___"
				opt := internal.NewRequestOptionsWithBindings(bindings)

				request, _ := graphsonv3.MakeRequestWithOptions("g.V(GDB___id)", opt)

				respFuture, err := conn.SubmitRequestAsync(request)
				So(err, ShouldBeNil)

				resp := respFuture.Get()
				So(resp.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_NO_CONTENT)
				So(resp.Data, ShouldBeNil)
			})

			Convey("send multi-dsl in one connect", func() {
				// send vertex count
				request1, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", nil)
				respFuture1, err := conn.SubmitRequestAsync(request1)
				So(err, ShouldBeNil)
				resp1 := respFuture1.Get()
				So(resp1.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)

				// send edge count
				request2, _ := graphsonv3.MakeRequestWithOptions("g.E().count()", nil)
				respFuture2, err := conn.SubmitRequestAsync(request2)
				So(err, ShouldBeNil)
				resp2 := respFuture2.Get()
				So(resp2.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)
			})

			Convey("send dsl after close", func() {
				conn.Close()
				So(conn.Disposed(), ShouldBeTrue)

				request, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", nil)

				_, err := conn.SubmitRequestAsync(request)
				So(err, ShouldNotBeNil)
			})

			Convey("send dsl with request id", func() {
				opt := internal.NewRequestOptionsWithBindings(nil)
				id, err := uuid.NewUUID()
				So(err, ShouldBeNil)
				opt.SetRequestId(id.String())

				request, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", opt)

				respFuture, err := conn.SubmitRequestAsync(request)
				So(err, ShouldBeNil)

				resp := respFuture.Get()
				So(resp.RequestID, ShouldEqual, id.String())
				So(resp.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)
			})

			Convey("send multi request in async with the same request id", func() {
				opt := internal.NewRequestOptionsWithBindings(nil)
				id, err := uuid.NewUUID()
				So(err, ShouldBeNil)
				opt.SetRequestId(id.String())

				request, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", opt)
				respFuture, err := conn.SubmitRequestAsync(request)
				So(err, ShouldBeNil)
				resp := respFuture.Get()
				So(resp.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)

				Convey("send multi request in pending", func() {
					request1, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", opt)
					respFuture1, err := conn.SubmitRequestAsync(request1)
					So(err, ShouldBeNil)

					request2, _ := graphsonv3.MakeRequestWithOptions("g.E().count()", opt)
					respFuture2, err := conn.SubmitRequestAsync(request2)
					So(err, ShouldBeNil)

					resp := respFuture1.Get()
					So(resp.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_SUCCESS)

					resp2 := respFuture2.Get()
					So(resp2.Code, ShouldEqual, graphsonv3.RESPONSE_STATUS_REQUEST_ERROR_DELIVER)
				})
			})

			conn.Close()
		})
	})
}
