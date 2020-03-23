/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2020/3/23
 */

package pool

import (
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
)

const requestIdMatchStr = "\"requestId\":\""
const requestIdEndMatchStr = "\",\"op\":"
const respPrefix = `{"requestId": "`
const respSuffix = `", "result": { "data": { "@type": "g:List", "@value": [ { "@type": "g:Int64", "@value": 0 } ] }, "meta": { "@type": "g:Map", "@value": [] } }, "status": { "attributes": { "@type": "g:Map", "@value": [] }, "code": 200, "message": "" } } `

type testGdbEchoServer struct {
	wsUpgrader         websocket.Upgrader
	wsServer           *httptest.Server
	wsEchoFun          http.HandlerFunc
	WsUrl              string
	WsMakeResponseFunc func(requestId string) []byte
}

func StartGdbTestServer() *testGdbEchoServer {
	server := &testGdbEchoServer{
		wsUpgrader: websocket.Upgrader{},
	}

	// make default response
	server.WsMakeResponseFunc = func(requestId string) []byte {
		response := respPrefix + requestId + respSuffix
		return []byte(response)
	}

	server.wsEchoFun = func(writer http.ResponseWriter, request *http.Request) {
		c, err := server.wsUpgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer c.Close()

		for {
			mt, message, err := c.ReadMessage()
			if err != nil {
				break
			}
			msg := string(message)
			var response []byte

			idIdx := strings.Index(msg, requestIdMatchStr)
			if idIdx > 0 {
				idEndIdx := strings.Index(msg, requestIdEndMatchStr)
				requestId := msg[idIdx+len(requestIdMatchStr) : idEndIdx]
				response = server.WsMakeResponseFunc(requestId)
			} else {
				response = []byte(msg)
			}
			err = c.WriteMessage(mt, response)
			if err != nil {
				break
			}
		}
	}
	server.wsServer = httptest.NewServer(server.wsEchoFun)
	server.WsUrl = "ws" + strings.TrimPrefix(server.wsServer.URL, "http")

	return server
}

func (server *testGdbEchoServer) CloseGdbTestServer() {
	server.wsServer.Close()
}
