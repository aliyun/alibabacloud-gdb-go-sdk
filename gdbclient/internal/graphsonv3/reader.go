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
	"encoding/json"
	"errors"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/graph"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal"
	"go.uber.org/zap"
)

const (
	// The server successfully processed a request to completion - threr are no messages remaining in this stream
	RESPONSE_STATUS_SUCCESS = 200

	// The server processed the request but there is no result to return
	RESPONSE_STATUS_NO_CONTENT = 204

	// The server successfully returned some content, but there is more in the stream to arrive - wait for the end of the stream
	RESPONSE_STATUS_PARITAL_CONTENT = 206

	// The server could not authenticate the request or the client requested a resource it did not have access to
	RESPONSE_STATUS_UNAUTHORIZED = 401

	// The server could authenticate the request, but will not fulfill it
	RESPONSE_STATUS_FORBIDDEN = 403

	// A challenge from the server for the client to authenticate its request
	RESPONSE_STATUS_AUTHENTICATE = 407

	// The request message contains objects that were not serializable on the client side
	RESPONSE_STATUS_REQUEST_ERROR_SERIALIZATION = 497

	// The request message was not properly formatted which means it could not be parsed at all or the "op" code was
	// not recognized such that Gremlin Server could properly route it for processing.  Check the message format and
	// retry the request.
	RESPONSE_STATUS_REQUEST_ERROR_MALFORMED_REQUEST = 498

	// The request message was parseable, but the arguments supplied in the message were in conflict or incomplete.
	// Check the message format and retry the request.
	RESPONSE_STATUS_REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS = 499

	// A general server error occurred that prevented the request from being processed
	RESPONSE_STATUS_SERVER_ERROR = 500

	// The script submitted for processing evaluated in the {@code ScriptEngine} with errors and could not be
	// processed.  Check the script submitted for syntax errors or other problems and then resubmit.
	RESPONSE_STATUS_SERVER_ERROR_SCRIPT_EVALUATION = 597

	// The server exceeded one of the timeout settings for the request and could therefore only partially responded
	// or did not respond at all.
	RESPONSE_STATUS_SERVER_ERROR_TIMEOUT = 598

	// The server was not capable of serializing an object that was returned from the script supplied on the request.
	// Either transform the object into something Gremlin Server can process within the script or install mapper
	// serialization classes to Gremlin Server.
	RESPONSE_STATUS_SERVER_ERROR_SERIALIZATION = 599

	// The client failed to deliver this request message to server
	RESPONSE_STATUS_REQUEST_ERROR_DELIVER = 697
)

type Response struct {
	Data      interface{}
	RequestID string
	Code      int
}

func NewErrorResponse(requestId string, code int, err error) *Response {
	return &Response{
		RequestID: requestId,
		Code:      code,
		Data:      err}
}

type responseStatusJson struct {
	Attributes json.RawMessage `json:"attributes"`
	Code       float64         `json:"code"`
	Message    string          `json:"message"`
}

type responseJson struct {
	RequestId string                     `json:"requestId"`
	Result    map[string]json.RawMessage `json:"result"`
	Status    responseStatusJson         `json:"status"`
}

var jsonUnmarshal = json.Unmarshal

func ReadResponse(msg []byte) (*Response, error) {
	if msg == nil {
		internal.Logger.Warn("response", zap.String("message", ""))
		return nil, nil
	}

	var respJson responseJson
	if err := jsonUnmarshal(msg, &respJson); err != nil {
		internal.Logger.Error("response", zap.String("message", string(msg)), zap.Error(err))
		return nil, internal.NewDeserializerError("response", msg, err)
	}

	status := respJson.Status
	result := respJson.Result
	response := &Response{Code: int(status.Code), RequestID: respJson.RequestId}

	if response.Code == RESPONSE_STATUS_AUTHENTICATE {
		return response, nil
	}

	if response.Code == RESPONSE_STATUS_SUCCESS || response.Code == RESPONSE_STATUS_PARITAL_CONTENT {
		response.Data = result["data"]

		// TODO: result["meta"]
	} else if response.Code == RESPONSE_STATUS_NO_CONTENT {
		response.Data = nil
	} else {
		// this is a "success" but represents no results otherwise it is an error
		message := status.Message
		ret, err := resultRouter(status.Attributes)
		if err != nil {
			internal.Logger.Error("response attributes", zap.Int("code", response.Code), zap.Error(err), zap.String("raw", string(status.Attributes)))
			response.Data = err
		} else {
			attributes := ret.(map[interface{}]interface{})
			stackTrace, ok := attributes[graph.STATUS_ATTRIBUTE_STACK_TRACE].(string)
			if !ok {
				internal.Logger.Error("response attributes stack trace", zap.Int("code", response.Code), zap.String("raw", string(status.Attributes)))
			}

			var execptions_str []string
			if exceptions, ok := attributes[graph.STATUS_ATTRIBUTE_EXCEPTIONS].([]interface{}); ok {
				execptions_str = make([]string, len(exceptions), len(exceptions))
				for i := 0; i < len(exceptions); i++ {
					if execptions_str[i], ok = exceptions[i].(string); !ok {
						internal.Logger.Error("response attributes stack trace", zap.Int("code", response.Code), zap.Int("idx", i), zap.String("raw", string(status.Attributes)))
					}
				}
			}
			// set errors to Data
			response.Data = internal.NewResponseError(response.Code, message, stackTrace, execptions_str)
		}
	}
	return response, nil
}

// get result from one whole response
func GetResult(response *Response) ([]interface{}, error) {
	if response.Code == RESPONSE_STATUS_SUCCESS {
		if raw, ok := response.Data.(json.RawMessage); ok {
			return getResult(raw)
		}
		// handle json list
		if rawList, ok := response.Data.([]json.RawMessage); ok {
			var resultMerge []interface{}
			for _, raw := range rawList {
				if results, err := getResult(raw); err == nil {
					resultMerge = append(resultMerge, results...)
				}
			}
			return resultMerge, nil
		}
		return nil, errors.New("un-handle response Data")
	} else if response.Code == RESPONSE_STATUS_NO_CONTENT {
		return nil, nil
	}

	return nil, response.Data.(error)
}
