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
	"time"
)

type ResponseFuture struct {
	originalRequest *Request
	response        *Response
	signalChan      chan struct{}
	isCompleted     bool
}

func NewResponseFuture(request *Request) *ResponseFuture {
	return &ResponseFuture{
		originalRequest: request,
		signalChan:      make(chan struct{}, 1),
		isCompleted:     false}
}

func (r *ResponseFuture) Complete(response *Response) {
	defer close(r.signalChan)

	r.isCompleted = true
	if response != nil {
		r.response = response
	}
	r.signalChan <- struct{}{}
}

func (r *ResponseFuture) Request() *Request {
	return r.originalRequest
}

func (r *ResponseFuture) IsCompleted() bool {
	return r.isCompleted
}

func (r *ResponseFuture) FixResponse(fn func(response *Response)) {
	if r.response == nil {
		r.response = &Response{RequestID: r.originalRequest.RequestID}
	}
	fn(r.response)
}

func (r *ResponseFuture) Get() *Response {
	if !r.isCompleted {
		<-r.signalChan
	}
	return r.response
}

func (r *ResponseFuture) GetOrTimeout(timeout time.Duration) (*Response, bool) {
	if r.isCompleted {
		return r.response, false
	}

	select {
	case <-time.After(timeout):
		return nil, true
	case <-r.signalChan:
		return r.response, false
	}
}
