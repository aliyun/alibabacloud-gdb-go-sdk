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

package graphsonv3

import (
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/graph"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var (
	resp_int64 = `
{
    "@type": "g:Int64",
    "@value": 64
}
`
	resp_int32 = `
{
    "@type": "g:Int32",
    "@value": 32
}

`
	resp_int8 = `
{
    "@type": "gx:Byte",
    "@value": 16
}
`

	resp_float = `
{
    "@type": "g:Float",
    "@value": 33.2
}
`
	resp_double = `
{
    "@type": "g:Double",
    "@value": 63.2
}
`

	resp_bool = `true`

	resp_string = `"Jack"`

	// g.V().values('name')
	resp_list_string = `
{
    "@type": "g:List",
    "@value": [
        "1",
        "2",
        "5"
    ]
}
`
	// g.V().values('age')
	resp_list_int64 = `
{
    "@type": "g:List",
    "@value": [
        {
            "@type": "g:Int64",
            "@value": 22
        },
        {
            "@type": "g:Int64",
            "@value": 32
        }
    ]
}
`
	// g.V().values()
	resp_list_mixed = `
{
    "@type": "g:List",
    "@value": [
        {
            "@type": "g:Int64",
            "@value": 22
        },
        true,
        "Jack",
        {
            "@type": "g:Double",
            "@value": 76.3
        }
    ]
}
`

	resp_list_empty = `
{
    "@type": "g:List",
    "@value": [ ]
}
`
	// g.V().valueMap(true)
	resp_map = `
{
    "@type": "g:Map",
    "@value": [
        {
            "@type": "g:T",
            "@value": "id"
        },
        "1",
        {
            "@type": "g:T",
            "@value": "label"
        },
        "test",
        "age",
        {
            "@type": "g:List",
            "@value": [
                {
                    "@type": "g:Int64",
                    "@value": 22
                }
            ]
        },
        "name",
        {
            "@type": "g:List",
            "@value": [
                "Jack"
            ]
        },
        "weight",
        {
            "@type": "g:List",
            "@value": [
                {
                    "@type": "g:Double",
                    "@value": 76.3
                }
            ]
        }
    ]
}
`

	// just test
	resp_set = `
{
    "@type": "g:Set",
    "@value": [
        "2019-11-24",
        {
            "@type": "g:Double",
            "@value": 0.58
        }
    ]
}
`

	// property
	resp_prop_double = `
{
    "@type": "g:Property",
    "@value": {
        "key": "love",
        "value": {
            "@type": "g:Double",
            "@value": 0.58
        }
    }
}
`
	resp_prop_string = `
{
    "@type": "g:Property",
    "@value": {
        "key": "time",
        "value": "2019-11-24"
    }
}
`
	resp_vertex_prop_double = `
{
    "@type": "g:VertexProperty",
    "@value": {
        "id": "1",
        "label": "weight",
        "value": {
            "@type": "g:Double",
            "@value": 76.3
        }
    }
}
`
	resp_vertex_prop_string = `
{
    "@type": "g:VertexProperty",
    "@value": {
        "id": "1",
        "label": "name",
        "value": "Jack"
    }
}
`
	resp_vertex_only = `
{
    "@type": "g:Vertex",
    "@value": {
        "id": "5",
        "label": "testd"
    }
}
`
	resp_vertex_with_prop = `
{
    "@type": "g:Vertex",
    "@value": {
        "id": "2",
        "label": "testb",
        "properties": {
            "age": [
                {
                    "@type": "g:VertexProperty",
                    "@value": {
                        "id": "2",
                        "label": "age",
                        "value": {
                            "@type": "g:Int64",
                            "@value": 32
                        }
                    }
                }
            ],
            "name": [
                {
                    "@type": "g:VertexProperty",
                    "@value": {
                        "id": "2",
                        "label": "name",
                        "value": "Luck"
                    }
                }
            ]
        }
    }
}
`
	resp_edge_only = `
{
    "@type": "g:Edge",
    "@value": {
        "id": "13",
        "inV": "2",
        "inVLabel": "testb",
        "label": "testc",
        "outV": "1",
        "outVLabel": "test"
    }
}
`
	resp_edge_with_prop = `
{
    "@type": "g:Edge",
    "@value": {
        "id": "12",
        "inV": "2",
        "inVLabel": "testb",
        "label": "testc",
        "outV": "1",
        "outVLabel": "test",
        "properties": {
            "love": {
                "@type": "g:Property",
                "@value": {
                    "key": "love",
                    "value": {
                        "@type": "g:Double",
                        "@value": 0.58
                    }
                }
            },
            "time": {
                "@type": "g:Property",
                "@value": {
                    "key": "time",
                    "value": "2019-11-24"
                }
            }
        }
    }
}
`
	resp_path = `
{
    "@type": "g:Path",
    "@value": {
        "labels": {
            "@type": "g:List",
            "@value": [
                {
                    "@type": "g:Set",
                    "@value": []
                },
                {
                    "@type": "g:Set",
                    "@value": []
                },
                {
                    "@type": "g:Set",
                    "@value": []
                },
                {
                    "@type": "g:Set",
                    "@value": []
                },
                {
                    "@type": "g:Set",
                    "@value": []
                },
                {
                    "@type": "g:Set",
                    "@value": []
                },
                {
                    "@type": "g:Set",
                    "@value": []
                }
            ]
        },
        "objects": {
            "@type": "g:List",
            "@value": [
                {
                    "@type": "g:Vertex",
                    "@value": {
                        "id": "1",
                        "label": "person"
                    }
                },
                {
                    "@type": "g:Edge",
                    "@value": {
                        "id": "9",
                        "inV": "3",
                        "inVLabel": "software",
                        "label": "created",
                        "outV": "1",
                        "outVLabel": "person"
                    }
                },
                {
                    "@type": "g:Vertex",
                    "@value": {
                        "id": "3",
                        "label": "software"
                    }
                },
                {
                    "@type": "g:Edge",
                    "@value": {
                        "id": "11",
                        "inV": "3",
                        "inVLabel": "software",
                        "label": "created",
                        "outV": "4",
                        "outVLabel": "person"
                    }
                },
                {
                    "@type": "g:Vertex",
                    "@value": {
                        "id": "4",
                        "label": "person"
                    }
                },
                {
                    "@type": "g:Edge",
                    "@value": {
                        "id": "10",
                        "inV": "5",
                        "inVLabel": "software",
                        "label": "created",
                        "outV": "4",
                        "outVLabel": "person"
                    }
                },
                {
                    "@type": "g:Vertex",
                    "@value": {
                        "id": "5",
                        "label": "software"
                    }
                }
            ]
        }
    }
}
`
	resp_bulkSet = `
{
    "@type": "g:BulkSet",
    "@value": [
        {
            "@type": "g:Vertex",
            "@value": {
                "id": "marko",
                "label": "person"
            }
        },
        {
            "@type": "g:Int64",
            "@value": 4
        },
        {
            "@type": "g:Vertex",
            "@value": {
                "id": "josh",
                "label": "person"
            }
        },
        {
            "@type": "g:Int64",
            "@value": 5
        }
    ]
}
`
)

func TestGetResult(t *testing.T) {
	Convey("get single value", t, func() {
		Convey("int64", func() {
			ret, err := resultRouter([]byte(resp_int64))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			So(ret, ShouldEqual, int64(64))
		})

		Convey("int32", func() {
			ret, err := resultRouter([]byte(resp_int32))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			So(ret, ShouldEqual, int32(32))
		})

		Convey("int8", func() {
			ret, err := resultRouter([]byte(resp_int8))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			So(ret, ShouldEqual, int8(16))
		})

		Convey("float", func() {
			ret, err := resultRouter([]byte(resp_float))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			So(ret, ShouldBeGreaterThanOrEqualTo, float32(33.1))
		})

		Convey("double", func() {
			ret, err := resultRouter([]byte(resp_double))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			So(ret, ShouldBeGreaterThanOrEqualTo, float64(63.1))
		})

		Convey("bool", func() {
			ret, err := resultRouter([]byte(resp_bool))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			So(ret, ShouldEqual, true)
		})

		Convey("string", func() {
			ret, err := resultRouter([]byte(resp_string))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			So(ret, ShouldEqual, "Jack")
		})

		Convey("unknown type", func() {
			ret, err := resultRouter([]byte("17.2"))
			So(err, ShouldNotBeNil)
			So(ret, ShouldBeNil)
		})
	})

	Convey("single list", t, func() {
		Convey("list string", func() {
			ret, err := resultRouter([]byte(resp_list_string))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			vlist, ok := ret.([]interface{})
			So(ok, ShouldBeTrue)
			So(len(vlist), ShouldEqual, 3)
			So(vlist[1], ShouldEqual, "2")
		})

		Convey("list int64", func() {
			ret, err := resultRouter([]byte(resp_list_int64))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			vlist, ok := ret.([]interface{})
			So(ok, ShouldBeTrue)
			So(len(vlist), ShouldEqual, 2)
			So(vlist[1], ShouldEqual, int64(32))
		})

		Convey("list mixed value", func() {
			ret, err := resultRouter([]byte(resp_list_mixed))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			vlist, ok := ret.([]interface{})
			So(ok, ShouldBeTrue)
			So(len(vlist), ShouldEqual, 4)

			So(vlist[0], ShouldEqual, int64(22))
			So(vlist[1], ShouldEqual, true)
		})

		Convey("list empty", func() {
			ret, err := resultRouter([]byte(resp_list_empty))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			vlist, ok := ret.([]interface{})
			So(ok, ShouldBeTrue)
			So(len(vlist), ShouldEqual, 0)
		})
	})

	Convey("single map", t, func() {
		Convey("value map", func() {
			ret, err := resultRouter([]byte(resp_map))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			vmap, ok := ret.(map[interface{}]interface{})
			So(ok, ShouldBeTrue)
			So(len(vmap), ShouldEqual, 5)

			So(vmap["id"], ShouldEqual, "1")
			So(vmap["label"], ShouldEqual, "test")

			age, ok := vmap["age"].([]interface{})
			So(ok, ShouldBeTrue)
			So(len(age), ShouldEqual, 1)
			So(age[0], ShouldEqual, int64(22))

			name, ok := vmap["name"].([]interface{})
			So(ok, ShouldBeTrue)
			So(len(name), ShouldEqual, 1)
			So(name[0], ShouldEqual, "Jack")

			weight, ok := vmap["weight"].([]interface{})
			So(ok, ShouldBeTrue)
			So(len(weight), ShouldEqual, 1)
			So(weight[0], ShouldBeBetween, float64(76.2), float64(76.4))
		})
	})

	Convey("single set", t, func() {
		Convey("set value", func() {
			ret, err := resultRouter([]byte(resp_set))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			// set is a list in golang driver
			vset, ok := ret.([]interface{})
			So(ok, ShouldBeTrue)
			So(len(vset), ShouldEqual, 2)

			So(vset[0], ShouldEqual, "2019-11-24")
			So(vset[1], ShouldBeBetween, float64(0.57), float64(0.59))
		})
	})

	Convey("single property", t, func() {
		Convey("double value", func() {
			ret, err := resultRouter([]byte(resp_prop_double))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			prop, ok := ret.(graph.Property)
			So(ok, ShouldBeTrue)
			So(prop.PKey(), ShouldEqual, "love")
			So(prop.PValue(), ShouldBeBetween, float64(0.57), float64(0.59))
		})

		Convey("string value", func() {
			ret, err := resultRouter([]byte(resp_prop_string))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			prop, ok := ret.(graph.Property)
			So(ok, ShouldBeTrue)
			So(prop.PKey(), ShouldEqual, "time")
			So(prop.PValue(), ShouldEqual, "2019-11-24")
		})
	})

	Convey("single vertex property", t, func() {
		Convey("double value", func() {
			ret, err := resultRouter([]byte(resp_vertex_prop_double))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			vprop, ok := ret.(graph.VertexProperty)
			So(ok, ShouldBeTrue)
			So(vprop.Id(), ShouldEqual, "1")
			So(vprop.PKey(), ShouldEqual, "weight")
			So(vprop.Label(), ShouldEqual, "weight")
			So(vprop.PValue(), ShouldBeBetween, float64(76.2), float64(76.4))
		})

		Convey("string value", func() {
			ret, err := resultRouter([]byte(resp_vertex_prop_string))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			vprop, ok := ret.(graph.VertexProperty)
			So(ok, ShouldBeTrue)
			So(vprop.Id(), ShouldEqual, "1")
			So(vprop.PKey(), ShouldEqual, "name")
			So(vprop.Label(), ShouldEqual, "name")
			So(vprop.PValue(), ShouldEqual, "Jack")
		})
	})

	Convey("single vertex", t, func() {
		Convey("vertex without property", func() {
			ret, err := resultRouter([]byte(resp_vertex_only))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			v, ok := ret.(graph.Vertex)
			So(ok, ShouldBeTrue)
			So(v.Id(), ShouldEqual, "5")
			So(v.Label(), ShouldEqual, "testd")
			So(len(v.Properties()), ShouldEqual, 0)
		})

		Convey("vertex with property", func() {
			ret, err := resultRouter([]byte(resp_vertex_with_prop))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			v, ok := ret.(graph.Vertex)
			So(ok, ShouldBeTrue)
			So(v.Id(), ShouldEqual, "2")
			So(v.Label(), ShouldEqual, "testb")

			So(len(v.Properties()), ShouldEqual, 2)

			vprop1 := v.VProperty("age")
			So(vprop1, ShouldNotBeNil)
			So(vprop1.Id(), ShouldEqual, v.Id())
			So(vprop1.PKey(), ShouldEqual, "age")
			So(vprop1.PValue(), ShouldEqual, int64(32))
			So(vprop1.VElement(), ShouldEqual, v)

			vprop2 := v.VProperty("name")
			So(vprop2, ShouldNotBeNil)
			So(vprop2.Id(), ShouldEqual, v.Id())
			So(vprop2.PKey(), ShouldEqual, "name")
			So(vprop2.PValue(), ShouldEqual, "Luck")
			So(vprop2.VElement(), ShouldEqual, v)
		})
	})

	Convey("single edge", t, func() {
		Convey("edge without property", func() {
			ret, err := resultRouter([]byte(resp_edge_only))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			e, ok := ret.(graph.Edge)
			So(ok, ShouldBeTrue)
			So(e.Id(), ShouldEqual, "13")
			So(e.Label(), ShouldEqual, "testc")
			So(e.InVertex().Id(), ShouldEqual, "2")
			So(e.OutVertex().Id(), ShouldEqual, "1")

			So(len(e.Properties()), ShouldEqual, 0)
		})

		Convey("edge with property", func() {
			ret, err := resultRouter([]byte(resp_edge_with_prop))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			e, ok := ret.(graph.Edge)
			So(ok, ShouldBeTrue)

			So(e.Id(), ShouldEqual, "12")
			So(e.Label(), ShouldEqual, "testc")
			So(e.InVertex().Id(), ShouldEqual, "2")
			So(e.OutVertex().Id(), ShouldEqual, "1")

			So(len(e.Properties()), ShouldEqual, 2)
			prop := e.Property("time")
			So(prop.PKey(), ShouldEqual, "time")
			So(prop.PValue(), ShouldEqual, "2019-11-24")
		})
	})

	Convey("single path", t, func() {
		Convey("path with vertex and edge", func() {
			ret, err := resultRouter([]byte(resp_path))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			p, ok := ret.(graph.Path)
			So(ok, ShouldBeTrue)
			So(p.Size(), ShouldEqual, 7)

			// labels should be emtpy
			labels := p.Labels()
			for _, l := range labels {
				So(len(l), ShouldEqual, 0)
			}

			// objects
			objects := p.Objects()

			v0, ok := objects[0].(graph.Vertex)
			So(ok, ShouldBeTrue)
			So(v0.Id(), ShouldEqual, "1")
			So(v0.Label(), ShouldEqual, "person")

			e1, ok := objects[1].(graph.Edge)
			So(ok, ShouldBeTrue)
			So(e1.Label(), ShouldEqual, "created")
			So(e1.OutVertex().Id(), ShouldEqual, "1")
			So(e1.InVertex().Id(), ShouldEqual, "3")

			v2, ok := objects[2].(graph.Vertex)
			So(ok, ShouldBeTrue)
			So(v2.Id(), ShouldEqual, "3")
			So(v2.Label(), ShouldEqual, "software")

			e3, ok := objects[3].(graph.Edge)
			So(ok, ShouldBeTrue)
			So(e3.Label(), ShouldEqual, "created")
			So(e3.OutVertex().Id(), ShouldEqual, "4")
			So(e3.InVertex().Id(), ShouldEqual, "3")

			v4, ok := objects[4].(graph.Vertex)
			So(ok, ShouldBeTrue)
			So(v4.Id(), ShouldEqual, "4")
			So(v4.Label(), ShouldEqual, "person")

			e5, ok := objects[5].(graph.Edge)
			So(ok, ShouldBeTrue)
			So(e5.Label(), ShouldEqual, "created")
			So(e5.OutVertex().Id(), ShouldEqual, "4")
			So(e5.InVertex().Id(), ShouldEqual, "5")

			v6, ok := objects[6].(graph.Vertex)
			So(ok, ShouldBeTrue)
			So(v6.Id(), ShouldEqual, "5")
			So(v6.Label(), ShouldEqual, "software")
		})
	})

	Convey("bulkSet", t, func() {
		Convey("bulkSet with vertex", func() {
			ret, err := resultRouter([]byte(resp_bulkSet))
			So(err, ShouldBeNil)
			So(ret, ShouldNotBeNil)

			b, ok := ret.(*graph.BulkSet)
			So(ok, ShouldBeTrue)

			So(b.UniqueSize(), ShouldEqual, 2)
			So(b.Size(), ShouldEqual, 9)
		})

	})
}
