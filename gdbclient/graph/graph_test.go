/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/12/3
 */

package graph

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestNewDetachedElement(t *testing.T) {
	Convey("create new property", t, func() {
		propKey := "prop_Key"
		propValue := "prop_Value"

		prop := NewDetachedProperty(propKey, propValue, nil)
		So(prop.PKey(), ShouldEqual, propKey)
		So(prop.PValue(), ShouldEqual, propValue)
		So(prop.PElement(), ShouldBeNil)

		So(prop.String(), ShouldEqual, "p[prop_Key->prop_Value]")
	})

	Convey("create new element", t, func() {
		element := NewDetachedElement("gdbId", "gdbLabel")

		So(element.Id(), ShouldEqual, "gdbId")
		So(element.Label(), ShouldEqual, "gdbLabel")
		So(element.properties, ShouldBeNil)
	})
}

func TestNewDetachedEdge(t *testing.T) {
	Convey("create new edge", t, func() {
		edge := NewDetachedEdge(NewDetachedElement("gdbId", "gdbLabel"))
		edge.AddProperty(NewDetachedProperty("time", "2019-11-29", nil))
		edge.AddProperty(NewDetachedProperty("is_delete", false, nil))

		So(edge.Id(), ShouldEqual, "gdbId")
		So(edge.Label(), ShouldEqual, "gdbLabel")
		So(edge.Property("is_delete").PValue(), ShouldEqual, false)
		So(len(edge.Properties()), ShouldEqual, 2)

		So(edge.String(), ShouldEqual, "e[gdbId][?-gdbLabel->?]")

		Convey("attach vertex to edge", func() {
			vertex1 := NewDetachedVertex(NewDetachedElement("gdbVId1", "gdbVLabel1"))
			vertex2 := NewDetachedVertex(NewDetachedElement("gdbVId2", "gdbVLabel2"))

			edge.SetVertex(true, vertex1)
			edge.SetVertex(false, vertex2)

			So(edge.OutVertex().Id(), ShouldEqual, "gdbVId1")
			So(edge.InVertex().Label(), ShouldEqual, "gdbVLabel2")

			So(edge.String(), ShouldEqual, "e[gdbId][gdbVId1-gdbLabel->gdbVId2]")
		})
	})
}

func TestNewDetachedEdgeWithProperty(t *testing.T) {
	Convey("create new edge with prop", t, func() {
		edge := NewDetachedEdge(NewDetachedElement("gdbId", "gdbLabel"))
		edge.AddProperty(NewDetachedProperty("time", "2019-11-29", nil))
		edge.AddProperty(NewDetachedProperty("is_delete", false, nil))

		So(len(edge.Properties()), ShouldEqual, 2)
		So(len(edge.Properties("time")), ShouldEqual, 1)

		// edge don't support SET property
		edge.AddProperty(NewDetachedProperty("time", "2019-11-30", nil))
		So(len(edge.Properties("time")), ShouldEqual, 1)
		So(edge.Property("time").PValue(), ShouldEqual, "2019-11-30")
	})
}

func TestNewDetachedVertex(t *testing.T) {
	Convey("create new vertex", t, func() {
		vertex := NewDetachedVertex(NewDetachedElement("gdbVId", "gdbVLabel"))

		So(vertex.Id(), ShouldEqual, "gdbVId")
		So(vertex.Label(), ShouldEqual, "gdbVLabel")

		So(vertex.String(), ShouldEqual, "v[gdbVId]")

		Convey("add vertex property", func() {
			vertex.AddProperty(NewDetachedVertexProperty(NewDetachedElement("gdbVId", "name"), "Jack"))
			vertex.AddProperty(NewDetachedVertexProperty(NewDetachedElement("gdbVId", "age"), 32))

			So(vertex.Property("name").PValue(), ShouldEqual, "Jack")
			So(vertex.VProperty("name").Id(), ShouldEqual, vertex.Id())
			So(vertex.VProperty("name").PKey(), ShouldEqual, "name")

			So(vertex.VProperty("age"), ShouldHaveSameTypeAs, vertex.Property("age"))

			So(vertex.String(), ShouldEqual, "v[gdbVId]")
		})
	})
}

func TestNewDetachedVertexWithProperty(t *testing.T) {
	Convey("create new vertex with prop", t, func() {
		vertex := NewDetachedVertex(NewDetachedElement("gdbVId", "gdbVLabel"))

		vprop := NewDetachedVertexProperty(NewDetachedElement("gdbVId", "name"), "Jack")
		vprop.SetVertex(vertex)
		vertex.AddProperty(vprop)

		vprop = NewDetachedVertexProperty(NewDetachedElement("gdbVId", "age"), 32)
		vprop.SetVertex(vertex)
		vertex.AddProperty(vprop)

		vprop = NewDetachedVertexProperty(NewDetachedElement("gdbVId", "name"), "Luck")
		vprop.SetVertex(vertex)
		vertex.AddProperty(vprop)

		So(len(vertex.VProperties("name")), ShouldEqual, 2)
		So(len(vertex.VProperties("age")), ShouldEqual, 1)

		So(len(vertex.Keys()), ShouldEqual, 2)
		So(len(vertex.Values()), ShouldEqual, 3)
		So(len(vertex.Values("name")), ShouldEqual, 2)
	})
}

func TestNewDetachedVertexProperty(t *testing.T) {
	Convey("create new vertex property", t, func() {
		vprop := NewDetachedVertexProperty(NewDetachedElement("gdbVId1", "name"), "Jack")

		So(vprop.Id(), ShouldEqual, "gdbVId1")
		So(vprop.PKey(), ShouldEqual, "name")
		So(vprop.PValue(), ShouldEqual, "Jack")

		So(vprop.String(), ShouldEqual, "vp[name->Jack]")

		Convey("attach to vertex", func() {
			vertex := NewDetachedVertex(NewDetachedElement("gdbVId1", "gdbVLabel1"))

			// double attach
			vprop.SetVertex(vertex)
			vertex.AddProperty(vprop)

			So(vprop.VElement().Label(), ShouldEqual, vertex.Label())
			So(vertex.Property("name").PElement(), ShouldEqual, vertex)

			So(vertex.String(), ShouldEqual, "v[gdbVId1]")
		})
	})
}

func TestNewDetachedPath(t *testing.T) {
	Convey("create new path", t, func() {
		v1 := NewDetachedVertex(NewDetachedElement("gdbVId1", "gdbVLabel"))
		v2 := NewDetachedVertex(NewDetachedElement("gdbVId2", "gdbVLabel"))

		e1 := NewDetachedEdge(NewDetachedElement("gdbIdE1", "gdbELabel"))

		e1.SetVertex(true, v1)
		e1.SetVertex(false, v2)

		path := NewDetachedPath()
		labels := make([]string, 1, 1)
		labels[0] = ""
		path.Extend(v1, labels)
		path.Extend(e1, labels)
		path.Extend(v2, labels)

		So(path.Size(), ShouldEqual, 3)
		So(path.String(), ShouldEqual, "path[v[gdbVId1],e[gdbIdE1][gdbVId1-gdbELabel->gdbVId2],v[gdbVId2]]")
	})
}

func TestNewBulkSet(t *testing.T) {
	Convey("create new bulkSet", t, func() {
		v1 := NewDetachedVertex(NewDetachedElement("gdbVId1", "gdbVLabel"))
		v2 := NewDetachedVertex(NewDetachedElement("gdbVId2", "gdbVLabel"))

		bulk := NewBulkSet()
		bulk.Add(v1, 20)
		bulk.Add(v2, 87)

		So(bulk.UniqueSize(), ShouldEqual, 2)
		So(bulk.Size(), ShouldEqual, 107)
		So(bulk.IsEmpty(), ShouldBeFalse)
		So(bulk.String(), ShouldEqual, "{{v[gdbVId1] : 20},{v[gdbVId2] : 87}}")
	})
}
