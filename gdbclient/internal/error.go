/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/12/2
 */

package internal

import (
	"strconv"
	"strings"
)

type ResponseError struct {
	code       int
	message    string
	stackTrace string
	exceptions []string
}

func NewResponseError(code int, message string, stackTrace string, exceptions []string) error {
	return &ResponseError{code: code, message: message, stackTrace: stackTrace, exceptions: exceptions}
}

func (r *ResponseError) Error() string {
	return fmtComma(
		fmtError("type", "RESPONSE_ERROR"),
		fmtError("code", strconv.FormatInt(int64(r.code), 10)),
		fmtError("message", r.message),
		fmtError("stackTrace", r.stackTrace),
		fmtSliceError("exceptions", r.exceptions),
	)
}

type DeserializerError struct {
	function string
	message  []byte
	err      error
}

func NewDeserializerError(function string, message []byte, err error) error {
	return &DeserializerError{function: function, message: message, err: err}
}

func (d *DeserializerError) Error() string {
	return fmtComma(
		fmtError("type", "Deserializer"),
		fmtError("function", d.function),
		fmtError("error", d.err.Error()),
	)
}

func fmtError(k, v string) string { return "{\"" + k + "\":\"" + v + "\"}" }

func fmtSliceError(k string, v []string) string {
	return "{\"" + k + "\":[\"" + strings.Join(v, "\",\"") + "\"]}"
}

func fmtComma(data ...string) string {
	var res string
	for i, d := range data {
		res += d
		if len(data) == i+1 {
			break
		}
		res += ","
	}
	return res
}
