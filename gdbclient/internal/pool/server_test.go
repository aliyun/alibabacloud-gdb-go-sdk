/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2020/3/3
 */

package pool

import (
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal/graphsonv3"
	"github.com/gorilla/websocket"
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
)

func TestServer(t *testing.T) {
	Convey("start websocket server", t, func() {
		server := StartGdbTestServer()
		So(server, ShouldNotBeNil)
		defer server.CloseGdbTestServer()

		Convey("connect and close", func() {
			ws, _, err := websocket.DefaultDialer.Dial(server.WsUrl, nil)
			So(err, ShouldBeNil)
			So(ws, ShouldNotBeNil)
			defer ws.Close()

			Convey("should be echo server when normal buffer", func() {
				testBuf := "test"

				// send a test buffer
				err = ws.WriteMessage(websocket.BinaryMessage, []byte(testBuf))
				So(err, ShouldBeNil)

				// should be echo of this buffer
				t, buf, err := ws.ReadMessage()
				So(err, ShouldBeNil)
				So(t, ShouldEqual, websocket.BinaryMessage)
				So(string(buf), ShouldEqual, testBuf)
			})

			Convey("should return graphson response with the same requestId", func() {
				request, _ := graphsonv3.MakeRequestWithOptions("g.V().count()", nil)
				outBuf, _ := graphsonv3.SerializerRequest(request)

				// send a graphson request
				err := ws.WriteMessage(websocket.BinaryMessage, outBuf)
				So(err, ShouldBeNil)

				t, buf, err := ws.ReadMessage()
				So(err, ShouldBeNil)
				So(t, ShouldEqual, websocket.BinaryMessage)

				So(strings.Contains(string(buf), request.RequestID), ShouldBeTrue)
			})
		})
	})
}
