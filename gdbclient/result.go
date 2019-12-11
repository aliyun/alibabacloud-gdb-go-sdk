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

package gdbclient

import (
	"errors"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/graph"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal/graphsonv3"
	"time"
)

type ResultSetFuture interface {
	IsCompleted() bool

	GetResults() ([]Result, error)

	GetResultsOrTimeout(timeout time.Duration) ([]Result, bool, error)
}

type _ResultSetFuture struct {
	future *graphsonv3.ResponseFuture
}

func (r *_ResultSetFuture) IsCompleted() bool {
	return r.future.IsCompleted()
}

func (r *_ResultSetFuture) GetResults() ([]Result, error) {
	results, err := graphsonv3.GetResult(r.future.Get())
	if err != nil {
		return nil, err
	}
	return r.returnResults(results), nil
}

func (r *_ResultSetFuture) GetResultsOrTimeout(timeout time.Duration) ([]Result, bool, error) {
	if response, ok := r.future.GetOrTimeout(timeout); ok {
		return nil, true, errors.New("get result timeout")
	} else {
		results, err := graphsonv3.GetResult(response)
		if err != nil {
			return nil, false, err
		}
		return r.returnResults(results), false, nil
	}
}

func (r *_ResultSetFuture) returnResults(results []interface{}) []Result {
	size := len(results)
	ret := make([]Result, size, size)
	for i := 0; i < size; i++ {
		ret[i].value = results[i]
	}
	return ret
}

func NewResultSetFuture(future *graphsonv3.ResponseFuture) ResultSetFuture {
	return &_ResultSetFuture{future: future}
}

type Result struct {
	value interface{}
}

func (r *Result) SetValue(value interface{}) {
	r.value = value
}

func (r *Result) GetObject() interface{} {
	return r.value
}

func (r *Result) GetBool() bool {
	if val, ok := r.value.(bool); ok {
		return val
	}
	return false
}

func (r *Result) GetInt8() int8 {
	if val, ok := r.value.(int8); ok {
		return val
	}
	return 0
}

func (r *Result) GetInt32() int32 {
	if val, ok := r.value.(int32); ok {
		return val
	}
	return 0
}

func (r *Result) GetInt64() int64 {
	if val, ok := r.value.(int64); ok {
		return val
	}
	return 0
}

func (r *Result) GetFloat() float32 {
	if val, ok := r.value.(float32); ok {
		return val
	}
	return 0
}

func (r *Result) GetDouble() float64 {
	if val, ok := r.value.(float64); ok {
		return val
	}
	return 0
}

func (r *Result) GetString() string {
	if val, ok := r.value.(string); ok {
		return val
	}
	return ""
}

func (r *Result) GetVertex() graph.Vertex {
	if val, ok := r.value.(graph.Vertex); ok {
		return val
	}
	return nil
}

func (r *Result) GetEdge() graph.Edge {
	if val, ok := r.value.(graph.Edge); ok {
		return val
	}
	return nil
}

func (r *Result) GetProperty() graph.Property {
	if val, ok := r.value.(graph.Property); ok {
		return val
	}
	return nil
}

func (r *Result) GetVertexProperty() graph.VertexProperty {
	if val, ok := r.value.(graph.VertexProperty); ok {
		return val
	}
	return nil
}
