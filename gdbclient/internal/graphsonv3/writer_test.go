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

package graphsonv3

import (
	"encoding/json"
	"errors"
	"github.com/aliyun/alibabacloud-gdb-go-client/gdbclient/internal"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestSerializerRequest(t *testing.T) {
	Convey("normal user request", t, func() {
		req := &Request{RequestID: "testId", Op: "eval", Args: make(map[string]interface{})}

		req.Args[internal.ARGS_GREMLIN] = "testDsl"

		Convey("serializer request", func() {
			msg, err := SerializerRequest(req)

			So(err, ShouldBeNil)
			So(msg, ShouldNotBeNil)
		})

		Convey("serializer request with json replace error", func() {
			defer func() {
				jsonMarshal = json.Marshal
			}()

			testErr := errors.New("testFailed")
			jsonMarshal = func(v interface{}) ([]byte, error) { return nil, testErr }

			msg, err := SerializerRequest(req)

			So(err, ShouldEqual, testErr)
			So(msg, ShouldBeNil)
		})
	})
}

func TestMakeRequestWithOptions(t *testing.T) {
	Convey("normal user request", t, func() {
		gremlin := "g.V().count()"

		Convey("request with nil options", func() {
			req, err := MakeRequestWithOptions(gremlin, nil)

			So(err, ShouldBeNil)

			So(req, ShouldNotBeNil)
			So(req.RequestID, ShouldNotBeEmpty)
			So(req.Op, ShouldEqual, internal.OPS_EVAL)
			So(req.Args[internal.ARGS_GREMLIN], ShouldEqual, gremlin)
		})

		Convey("request with options", func() {
			options := internal.NewRequestOptionsWithBindings(nil)
			options.AddArgs(internal.ARGS_SCRIPT_EVAL_TIMEOUT, 300)

			Convey("options with nil request id", func() {
				req, err := MakeRequestWithOptions(gremlin, options)

				So(err, ShouldBeNil)
				So(req, ShouldNotBeNil)
				So(req.RequestID, ShouldNotBeEmpty)
				So(req.Args[internal.ARGS_SCRIPT_EVAL_TIMEOUT], ShouldEqual, 300)
			})

			Convey("options with request id and bindings", func() {
				options.SetRequestId("testId")

				bindings := make(map[string]interface{})
				bindings["testParam1"] = "testValue"
				bindings["testParam2"] = 20
				options.AddArgs(internal.ARGS_BINDINGS, bindings)

				req, err := MakeRequestWithOptions(gremlin, options)
				So(err, ShouldBeNil)
				So(req, ShouldNotBeNil)
				So(req.RequestID, ShouldEqual, "testId")

				So(req.Args[internal.ARGS_BINDINGS], ShouldNotBeNil)
				read_bindings := req.Args[internal.ARGS_BINDINGS].(map[string]interface{})
				So(read_bindings, ShouldContainKey, "testParam1")
				So(read_bindings, ShouldContainKey, "testParam2")
			})
		})

		Convey("request with session options", func() {
			options := internal.NewRequestOptionsWithBindings(nil)

			options.AddArgs(internal.ARGS_SESSION, "session_id_33297979233")
			options.AddArgs(internal.ARGS_MANAGE_TRANSACTION, true)

			Convey("option without bindings", func() {
				req, err := MakeRequestWithOptions(gremlin, options)

				So(err, ShouldBeNil)
				So(req, ShouldNotBeNil)
				So(req.Processor, ShouldEqual, "session")
				So(req.Args[internal.ARGS_SESSION], ShouldEqual, "session_id_33297979233")
			})

			Convey("request to close session", func() {
				req := MakeRequestCloseSession("session_id_33297979233")

				So(req.Processor, ShouldEqual, "session")
				So(req.Op, ShouldEqual, internal.OPS_CLOSE)
			})
		})
	})
}

func TestMakeAuthRequest(t *testing.T) {
	Convey("auth request with id, username and password", t, func() {
		id := "testId"
		username := "zhiyan"
		password := "zhiyan"

		Convey("auth request is prepared", func() {
			req, err := MakeAuthRequest(id, username, password)

			Convey("The request should not be nil", func() {
				So(req, ShouldNotBeNil)
			})

			Convey("The error should be nil", func() {
				So(err, ShouldBeNil)
			})

			Convey("The request id should equal", func() {
				So(req.RequestID == id, ShouldBeTrue)
			})

			Convey("The request op is special", func() {
				So(req.Op, ShouldEqual, internal.OPS_AUTHENTICATION)
			})
		})
	})
}
