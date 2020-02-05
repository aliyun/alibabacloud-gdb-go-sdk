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

		element.SetProperty("name", NewDetachedProperty("name", "Jack", nil))
		element.SetProperty("age", NewDetachedProperty("age", 32, nil))
		element.SetProperty("weight", NewDetachedProperty("weight", 67.2, nil))

		So(element.Id(), ShouldEqual, "gdbId")
		So(element.Label(), ShouldEqual, "gdbLabel")

		So(element.Property("name").PKey(), ShouldEqual, "name")
		So(element.Property("name").PValue(), ShouldEqual, "Jack")

		So(element.Value("age"), ShouldEqual, 32)

		So(len(element.Keys()), ShouldEqual, 3)
		So(len(element.Values()), ShouldEqual, 3)
		So(len(element.Properties()), ShouldEqual, 3)

		So(element.Values("weight", "name")[0], ShouldEqual, 67.2)
	})
}

func TestNewDetachedEdge(t *testing.T) {
	Convey("create new edge", t, func() {
		edge := NewDetachedEdge(NewDetachedElement("gdbId", "gdbLabel"))
		edge.SetProperty("time", NewDetachedProperty("time", "2019-11-29", nil))
		edge.SetProperty("is_delete", NewDetachedProperty("is_delete", false, nil))

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

func TestNewDetachedVertex(t *testing.T) {
	Convey("create new vertex", t, func() {
		vertex := NewDetachedVertex(NewDetachedElement("gdbVId", "gdbVLabel"))

		So(vertex.Id(), ShouldEqual, "gdbVId")
		So(vertex.Label(), ShouldEqual, "gdbVLabel")

		So(vertex.String(), ShouldEqual, "v[gdbVId]")

		Convey("add vertex property", func() {
			vertex.SetProperty("name", NewDetachedProperty("name", "Jack", nil))
			vertex.SetProperty("age", NewDetachedProperty("age", 32, nil))

			So(vertex.Property("name").PValue(), ShouldEqual, "Jack")
			So(vertex.VProperty("name").Id(), ShouldEqual, vertex.Id())
			So(vertex.VProperty("name").PKey(), ShouldEqual, "name")

			So(vertex.VProperty("age"), ShouldHaveSameTypeAs, vertex.Property("age"))

			So(vertex.VProperty("age").VElement(), ShouldEqual, vertex)
			So(vertex.VProperty("age").PElement(), ShouldEqual, vertex)

			So(vertex.String(), ShouldEqual, "v[gdbVId]")
		})
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
			vertex.SetProperty(vprop.PKey(), vprop)

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
