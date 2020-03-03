/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/11/20
 */

package graphsonv3

import (
	"encoding/json"
	"errors"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/graph"
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal"
	"go.uber.org/zap"
)

type result struct {
	Type  string          `json:"@type"`
	Value json.RawMessage `json:"@value"`
}

type vertexPropertyV3 struct {
	Id    string          `json:"id"`
	Value json.RawMessage `json:"value"`
	Label string          `json:"label"`
}

type vertexV3 struct {
	Id         string              `json:"id"`
	Label      string              `json:"label"`
	Properties map[string][]result `json:"properties,omitempty"`
}

type propertyV3 struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type edgeV3 struct {
	Id         string            `json:"id"`
	Label      string            `json:"label"`
	InV        string            `json:"inV"`
	InVLabel   string            `json:"inVLabel"`
	OutV       string            `json:"outV"`
	OutVLabel  string            `json:"outVLabel"`
	Properties map[string]result `json:"properties,omitempty"`
}

type pathV3 struct {
	Labels  result `json:"labels"`
	Objects result `json:"objects"`
}

type getResultHandler func(r *result) (interface{}, error)

const (
	gTypeBool   = "g:Bool" // no bool type in '@type'
	gTypeInt8   = "gx:Byte"
	gTypeInt16  = "g:Int16" // not support short in GDB
	gTypeInt32  = "g:Int32"
	gTypeInt64  = "g:Int64"
	gTypeFloat  = "g:Float"
	gTypeDouble = "g:Double"
	gTypeString = "g:String" // no string type in '@type'

	gTypeList    = "g:List"
	gTypeMap     = "g:Map"
	gTypeSet     = "g:Set"
	gTypeBulkSet = "g:BulkSet"

	gTypeT = "g:T" // gremlin graph element type string

	gTypeVertex         = "g:Vertex"
	gTypeEdge           = "g:Edge"
	gTypeVertexProperty = "g:VertexProperty"
	gTypeProperty       = "g:Property"
	gTypePath           = "g:Path"
)

var resultRouterMap map[string]getResultHandler

func init() {
	resultRouterMap = map[string]getResultHandler{
		gTypeInt8:           getInt8,
		gTypeInt32:          getInt32,
		gTypeInt64:          getInt64,
		gTypeFloat:          getFloat,
		gTypeDouble:         getDouble,
		gTypeT:              getT,
		gTypeList:           getList,
		gTypeMap:            getMap,
		gTypeSet:            getSet,
		gTypeBulkSet:        getBulkSet,
		gTypeVertex:         getVertex,
		gTypeEdge:           getEdge,
		gTypeVertexProperty: getVertexProperty,
		gTypeProperty:       getProperty,
		gTypePath:           getPath,
	}
}

// main route for all json string
func getResult(raw json.RawMessage) ([]interface{}, error) {
	var r result
	if err := jsonUnmarshal(raw, &r); err != nil {
		return nil, err
	}

	// response start with 'g:List'
	if r.Type != gTypeList {
		internal.Logger.Error("graphSonV3 response start", zap.String("start type", r.Type))
		return nil, errors.New("response starts with not 'List'")
	}

	return resultListRouter(r.Value)
}

// result list
func resultListRouter(raw json.RawMessage) ([]interface{}, error) {
	results := make([]interface{}, 0)

	var j []json.RawMessage
	err := json.Unmarshal(raw, &j)
	if err != nil {
		internal.Logger.Error("graphSonV3 error", zap.String("raw", string(raw)), zap.Error(err))
		return nil, err
	}

	for _, jj := range j {
		if jj == nil {
			continue
		}
		if n, err := resultRouter(jj); err == nil {
			results = append(results, n)
		}
	}

	return results, nil
}

// result single
func resultRouter(raw json.RawMessage) (interface{}, error) {
	var j result
	if err := json.Unmarshal(raw, &j); err == nil {
		if router, ok := resultRouterMap[j.Type]; ok {
			return router(&j)
		} else {
			internal.Logger.Error("graphSonV3 unknown type", zap.String("type", j.Type), zap.String("raw", string(raw)))
			return nil, errors.New("un-support type :" + j.Type)
		}
	} else {
		return getBoolOrString(raw)
	}

	internal.Logger.Error("graphSonV3 un-handle response", zap.String("raw", string(raw)))
	return nil, internal.NewDeserializerError("single result", raw, nil)
}

func getBoolOrString(raw json.RawMessage) (interface{}, error) {
	var vstr string
	if err := json.Unmarshal(raw, &vstr); err == nil {
		return vstr, nil
	}

	var vbool bool
	if err := json.Unmarshal(raw, &vbool); err == nil {
		return vbool, nil
	}

	internal.Logger.Error("graphSonV3 un-handle response", zap.String("raw", string(raw)))
	return nil, internal.NewDeserializerError("single bool or string", raw, nil)
}

// T type should be string value
func getT(r *result) (interface{}, error) {
	var vstr string
	err := json.Unmarshal(r.Value, &vstr)
	return vstr, err
}

func getListBoolOrString(raw json.RawMessage) ([]interface{}, error) {
	var vstr []string
	if err := json.Unmarshal(raw, &vstr); err == nil {
		results := make([]interface{}, len(vstr))
		for i, v := range vstr {
			results[i] = v
		}
		return results, nil
	}

	var vbool []bool
	if err := json.Unmarshal(raw, &vbool); err == nil {
		results := make([]interface{}, len(vbool))
		for i, v := range vbool {
			results[i] = v
		}
		return results, nil
	}

	internal.Logger.Error("graphSonV3 un-handle response", zap.String("raw", string(raw)))
	return nil, internal.NewDeserializerError("list bool or string", raw, nil)
}

func getNumber(r *result) (float64, error) {
	v := 0.0
	err := json.Unmarshal(r.Value, &v)
	if err != nil {
		internal.Logger.Error("graphSonV3 un-handle number", zap.String("raw", string(r.Value)))
		return 0, internal.NewDeserializerError("number", r.Value, err)
	}
	return v, nil
}

func getInt8(r *result) (interface{}, error) {
	v, err := getNumber(r)
	return int8(v), err
}

func getInt32(r *result) (interface{}, error) {
	v, err := getNumber(r)
	return int32(v), err
}

func getInt64(r *result) (interface{}, error) {
	v, err := getNumber(r)
	return int64(v), err
}

func getFloat(r *result) (interface{}, error) {
	v, err := getNumber(r)
	return float32(v), err
}

func getDouble(r *result) (interface{}, error) {
	return getNumber(r)
}

func getList(r *result) (interface{}, error) {
	return resultListRouter(r.Value)
}

// ugly 'set' serializer
// and return list([]interface{}) due to no set in golang
func getSet(r *result) (interface{}, error) {
	return resultListRouter(r.Value)
}

// ugly 'map' serializer
func getMap(r *result) (interface{}, error) {
	result := make(map[interface{}]interface{})

	v, err := resultListRouter(r.Value)
	if err != nil {
		return nil, err
	}

	if len(v)%2 != 0 {
		// nu-pair map key-value
		return nil, internal.NewDeserializerError("map", r.Value, errors.New("un-pair map"))
	}

	for i := 0; i < len(v); {
		key := v[i]
		i++
		value := v[i]
		i++

		result[key] = value
	}

	return result, nil
}

func getBulkSet(r *result) (interface{}, error) {
	result := graph.NewBulkSet()

	v, err := resultListRouter(r.Value)
	if err != nil {
		return nil, err
	}

	if len(v)%2 != 0 {
		// nu-pair bulkSet key-value
		return nil, internal.NewDeserializerError("bulkSet", r.Value, errors.New("un-pair bulkSet"))
	}

	for i := 0; i < len(v); {
		key := v[i]
		i++
		value := v[i]
		i++

		if vp, ok := value.(int64); ok {
			result.Add(key, vp)
		} else {
			internal.Logger.Error("graphSonV3 bulkSet value type error", zap.Any("value", value))
		}
	}
	return result, nil
}

func getVertexProperty(r *result) (interface{}, error) {
	v := &vertexPropertyV3{}
	err := json.Unmarshal(r.Value, v)
	if err != nil {
		return nil, internal.NewDeserializerError("vertexProperty", r.Value, err)
	}

	props, err := resultRouter(v.Value)
	if err != nil {
		return nil, err
	}

	vp := graph.NewDetachedVertexProperty(graph.NewDetachedElement(v.Id, v.Label), props)
	return vp, nil
}

func getVertex(r *result) (interface{}, error) {
	v := &vertexV3{}
	err := json.Unmarshal(r.Value, v)
	if err != nil {
		return nil, internal.NewDeserializerError("vertex", r.Value, err)
	}

	vertex := graph.NewDetachedVertex(graph.NewDetachedElement(v.Id, v.Label))

	for _, props := range v.Properties {
		for _, prop := range props {
			p, err := getVertexProperty(&prop)
			if err != nil {
				return nil, err
			}

			if vp, ok := p.(*graph.DetachedVertexProperty); ok {
				// attach vertex to this prop
				vp.SetVertex(vertex)

				// add prop to vertex element
				vertex.AddProperty(vp)
			}
		}
	}

	return vertex, nil
}

func getProperty(r *result) (interface{}, error) {
	v := &propertyV3{}
	err := json.Unmarshal(r.Value, v)
	if err != nil {
		return nil, internal.NewDeserializerError("property", r.Value, err)
	}

	n, err := resultRouter(v.Value)
	if err != nil {
		return nil, err
	}

	prop := graph.NewDetachedProperty(v.Key, n, nil)
	return prop, nil
}

func getEdge(r *result) (interface{}, error) {
	v := &edgeV3{}
	err := json.Unmarshal(r.Value, v)
	if err != nil {
		return nil, internal.NewDeserializerError("edge", r.Value, err)
	}

	inVertex := graph.NewDetachedVertex(graph.NewDetachedElement(v.InV, v.InVLabel))
	outVertex := graph.NewDetachedVertex(graph.NewDetachedElement(v.OutV, v.OutVLabel))

	edge := graph.NewDetachedEdge(graph.NewDetachedElement(v.Id, v.Label))
	edge.SetVertex(true, outVertex)
	edge.SetVertex(false, inVertex)

	for _, prop := range v.Properties {
		p, err := getProperty(&prop)
		if err != nil {
			return nil, err
		}

		if pp, ok := p.(*graph.DetachedProperty); ok {
			edge.AddProperty(pp)
		}
	}

	return edge, nil
}

func getPath(r *result) (interface{}, error) {
	v := &pathV3{}
	err := json.Unmarshal(r.Value, v)
	if err != nil {
		return nil, internal.NewDeserializerError("path", r.Value, err)
	}

	if v.Labels.Type != gTypeList || v.Objects.Type != gTypeList {
		return nil, internal.NewDeserializerError("path", r.Value, errors.New("inner type error"))
	}

	objects, err := resultListRouter(v.Objects.Value)
	if err != nil {
		return nil, err
	}

	labels, err := resultListRouter(v.Labels.Value)
	if err != nil {
		return nil, err
	}

	// check size
	if len(objects) != len(labels) {
		return nil, internal.NewDeserializerError("path", r.Value, errors.New("un-pair labels and objects"))
	}

	path := graph.NewDetachedPath()
	for i := 0; i < len(objects); i++ {
		var labels_str []string

		label := labels[i].([]interface{})
		labels_str = make([]string, len(label), len(label))

		for i := 0; i < len(label); i++ {
			labels_str[i] = label[i].(string)
		}
		path.Extend(objects[i], labels_str)
	}
	return path, nil
}
