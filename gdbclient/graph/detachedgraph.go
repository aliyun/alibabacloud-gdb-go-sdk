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

package graph

import (
	"fmt"
	"strings"
)

// gremlin element class for reference
type DetachedElement struct {
	id         string
	label      string
	properties map[string]Property
}

func NewDetachedElement(id string, label string) *DetachedElement {
	return &DetachedElement{id: id, label: label, properties: map[string]Property{}}
}

func (d *DetachedElement) SetProperty(key string, prop Property) {
	d.properties[key] = prop
}

func (d *DetachedElement) Id() string {
	return d.id
}

func (d *DetachedElement) Label() string {
	return d.label
}

func (d *DetachedElement) Property(key string) Property {
	if d.properties == nil {
		return nil
	}

	if v, ok := d.properties[key]; ok {
		return v
	}
	return nil
}

func (d *DetachedElement) Properties(keys ...string) []Property {
	if d.properties == nil {
		return nil
	}

	var props []Property
	if keys == nil {
		for _, v := range d.properties {
			props = append(props, v)
		}
	} else {
		for _, key := range keys {
			if v, ok := d.properties[key]; ok {
				props = append(props, v)
			}
		}
	}
	return props
}

func (d *DetachedElement) Value(key string) interface{} {
	if d.properties == nil {
		return nil
	}

	if v, ok := d.properties[key]; ok {
		return v.PValue()
	}
	return nil
}

func (d *DetachedElement) Values(keys ...string) []interface{} {
	if d.properties == nil {
		return nil
	}

	var values []interface{}
	if keys == nil {
		for _, v := range d.properties {
			values = append(values, v.PValue())
		}
	} else {
		for _, key := range keys {
			if v, ok := d.properties[key]; ok {
				values = append(values, v.PValue())
			}
		}
	}
	return values
}

func (d *DetachedElement) Keys() []string {
	if d.properties == nil {
		return nil
	}

	i := 0
	keys := make([]string, len(d.properties), len(d.properties))
	for k := range d.properties {
		keys[i] = k
		i++
	}
	return keys
}

// gremlin property class for reference
type DetachedProperty struct {
	key     string
	value   interface{}
	element *DetachedElement
}

func NewDetachedProperty(key string, value interface{}, element *DetachedElement) *DetachedProperty {
	return &DetachedProperty{key: key, value: value, element: element}
}

func (d *DetachedProperty) PKey() string {
	return d.key
}

func (d *DetachedProperty) PValue() interface{} {
	return d.value
}

func (d *DetachedProperty) PElement() Element {
	return d.element
}

func (d *DetachedProperty) String() string {
	v := fmt.Sprint(d.value)
	if len(v) > 20 {
		v = v[:20] + "..."
	}
	return "p[" + d.key + "->" + v + "]"
}

// gremlin vertex class for reference
type DetachedVertex struct {
	*DetachedElement
}

func NewDetachedVertex(element *DetachedElement) *DetachedVertex {
	return &DetachedVertex{DetachedElement: element}
}

func (d *DetachedVertex) Edges(out bool, label ...string) []Edge {
	return nil
}

func (d *DetachedVertex) Vertices(out bool, label ...string) []Vertex {
	return nil
}

func (d *DetachedVertex) Property(key string) Property {
	return d.VProperty(key)
}

func (d *DetachedVertex) Properties(keys ...string) []Property {
	i := 0
	vprops := d.VProperties(keys...)
	props := make([]Property, len(vprops), len(vprops))
	for _, p := range vprops {
		props[i] = p
		i++
	}
	return props
}

func (d *DetachedVertex) VProperty(key string) VertexProperty {
	if d.properties == nil {
		return nil
	}

	if v, ok := d.properties[key]; ok {
		if vp, ok := v.(VertexProperty); ok {
			return vp
		} else {
			// here 'v' is Property
			return &DetachedVertexProperty{
				vertex:          d,
				value:           v.PValue(),
				DetachedElement: &DetachedElement{id: d.Id(), label: v.PKey()}}
		}
	}
	return nil
}

func (d *DetachedVertex) VProperties(keys ...string) []VertexProperty {
	if d.properties == nil {
		return nil
	}

	var vprops []VertexProperty
	if keys == nil {
		for k := range d.properties {
			vprops = append(vprops, d.VProperty(k))
		}
	} else {
		for _, k := range keys {
			vprops = append(vprops, d.VProperty(k))
		}
	}
	return vprops
}

func (d *DetachedVertex) String() string {
	return "v[" + d.id + "]"
}

// gremlin vertexProperty class for reference
type DetachedVertexProperty struct {
	*DetachedElement

	value  interface{}
	vertex *DetachedVertex
}

func NewDetachedVertexProperty(element *DetachedElement, value interface{}) *DetachedVertexProperty {
	return &DetachedVertexProperty{DetachedElement: element, value: value}
}

func (d *DetachedVertexProperty) SetVertex(v *DetachedVertex) {
	d.vertex = v
}

func (d *DetachedVertexProperty) PKey() string {
	return d.Label()
}

func (d *DetachedVertexProperty) PValue() interface{} {
	return d.value
}

func (d *DetachedVertexProperty) PElement() Element {
	return d.vertex
}

func (d *DetachedVertexProperty) VLabel() string {
	return d.Label()
}

func (d *DetachedVertexProperty) VElement() Vertex {
	return d.vertex
}

func (d *DetachedVertexProperty) String() string {
	v := fmt.Sprint(d.value)
	if len(v) > 20 {
		v = v[:20] + "..."
	}
	return "vp[" + d.label + "->" + v + "]"
}

// gremlin vertexProperty class for reference
type DetachedEdge struct {
	*DetachedElement

	outVertex *DetachedVertex
	inVertex  *DetachedVertex
}

func NewDetachedEdge(element *DetachedElement) *DetachedEdge {
	return &DetachedEdge{DetachedElement: element}
}

func (d *DetachedEdge) SetVertex(out bool, vertex *DetachedVertex) {
	if out {
		d.outVertex = vertex
	} else {
		d.inVertex = vertex
	}
}

func (d *DetachedEdge) InVertex() Vertex {
	return d.inVertex
}

func (d *DetachedEdge) OutVertex() Vertex {
	return d.outVertex
}

func (d *DetachedEdge) String() string {
	if d.inVertex != nil && d.outVertex != nil {
		return "e[" + d.id + "][" + d.inVertex.id + "-" + d.label + "->" + d.outVertex.id + "]"
	}
	return "e[" + d.id + "]"
}

type DetachedPath struct {
	objects []interface{}
	labels  [][]string
}

func NewDetachedPath() *DetachedPath {
	return &DetachedPath{objects: make([]interface{}, 0), labels: make([][]string, 0)}
}

func (d *DetachedPath) Extend(object interface{}, labels []string) {
	d.objects = append(d.objects, object)
	d.labels = append(d.labels, labels)
}

func (d *DetachedPath) Size() int {
	return len(d.objects)
}

func (d *DetachedPath) Objects() []interface{} {
	return d.objects
}

func (d *DetachedPath) Labels() [][]string {
	return d.labels
}

func (d *DetachedPath) String() string {
	var output []string
	for _, o := range d.objects {
		output = append(output, fmt.Sprint(o))
	}
	return fmt.Sprintf("path[%s]", strings.Join(output, ","))
}
