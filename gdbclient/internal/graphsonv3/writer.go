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

package graphsonv3

import (
	"encoding/base64"
	"encoding/json"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/graph"
	"github.com/google/uuid"
)

const GraphsonV3 = "!application/vnd.gremlin-v3.0+json"

var (
	// GenUUID is a monkey patched function for the Google UUIDv4 generator.
	GenUUID = uuid.NewUUID
	// jsonMarshal is a monkey patched function for the standard json.Marshal.
	jsonMarshal = json.Marshal
)

type Request struct {
	RequestID string                 `json:"requestId"`
	Op        string                 `json:"op"`
	Processor string                 `json:"processor"`
	Args      map[string]interface{} `json:"args"`
}

func SerializerRequest(request *Request) ([]byte, error) {
	// Formats request into byte format
	j, err := jsonMarshal(request)
	if err != nil {
		return nil, err
	}

	msg := []byte(GraphsonV3)
	msg = append(msg, j...)

	return msg, nil
}

func MakeRequestCloseSession(sessionId string) *Request {
	request := &Request{Op: graph.OPS_CLOSE, Args: make(map[string]interface{})}

	id, _ := GenUUID()
	request.RequestID = id.String()

	request.Processor = "session"
	request.Args[graph.ARGS_SESSION] = sessionId
	request.Args[graph.ARGS_GREMLIN] = "session.close()"

	return request
}

func MakeRequestWithOptions(gremlin string, options *graph.RequestOptions) (*Request, error) {
	request := &Request{Op: graph.OPS_EVAL, Args: make(map[string]interface{})}

	// override requestId
	if options != nil {
		request.RequestID = options.GetOverrideRequestId()
	}
	if request.RequestID == "" {
		if id, err := GenUUID(); err != nil {
			return nil, err
		} else {
			request.RequestID = id.String()
		}
	}
	// set specific configurations
	request.Args[graph.ARGS_GREMLIN] = gremlin
	request.Args[graph.ARGS_LANGUAGE] = "gremlin-groovy"

	// send request now if options is nil
	if options == nil {
		return request, nil
	}

	// set optional args if they were made available
	if timeout := options.GetTimeout(); timeout > 0 {
		request.Args[graph.ARGS_SCRIPT_EVAL_TIMEOUT] = timeout
	}

	session := false
	if args := options.GetArgs(); args != nil && len(args) > 0 {
		for k, v := range args {
			request.Args[k] = v
			if k == graph.ARGS_SESSION {
				session = true
			}
		}
	}
	// set 'session' processor if choose session mode
	if session {
		request.Processor = "session"
	}

	//internal.Logger.Info("request", zap.String("id", request.RequestID), zap.Bool("session", session))
	return request, nil
}

func MakeAuthRequest(requestId string, username string, password string) (*Request, error) {
	simpleAuth := make([]byte, len(username)+len(password)+2)
	copy(simpleAuth[1:], username)
	copy(simpleAuth[len(username)+2:], password)

	args := make(map[string]interface{})
	args[graph.ARGS_SASL] = base64.StdEncoding.EncodeToString(simpleAuth)

	request := &Request{
		RequestID: requestId,
		Op:        graph.OPS_AUTHENTICATION,
		Processor: "traversal",
		Args:      args,
	}
	return request, nil
}
