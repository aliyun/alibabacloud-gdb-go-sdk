/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/11/28
 */

package graphsonv3

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var (
	resp200_count = `
{
    "requestId": "ec36254a-b4d5-48a0-854c-d0e2547cfb1b",
    "result": {
        "data": {
            "@type": "g:List",
            "@value": [
                {
                    "@type": "g:Int64",
                    "@value": 0
                }
            ]
        },
        "meta": {
            "@type": "g:Map",
            "@value": []
        }
    },
    "status": {
        "attributes": {
            "@type": "g:Map",
            "@value": []
        },
        "code": 200,
        "message": ""
    }
}
`
	resp204_empty = `
{
    "requestId": "a5b9a631-a971-4bcf-ba65-80c313525a78",
    "result": {
        "data": {
            "@type": "g:List",
            "@value": []
        },
        "meta": {
            "@type": "g:Map",
            "@value": []
        }
    },
    "status": {
        "attributes": {
            "@type": "g:Map",
            "@value": []
        },
        "code": 204,
        "message": ""
    }
}
`
	resp_failed = `
{
    "requestId": "a5b9a631-a971-4bcf-ba65-80c313525a78",
    "result": {
        "data": {
            "@type": "g:List",
            "@value": []
        },
        "meta": {
            "@type": "g:Map",
            "@value": []
        }
    },
    "status": {
        "attributes": {
            "@type": "g:Map",
            "@value": []
        },
        "code": "204",
        "message": ""
    }
}
`
	resp_error = `
{
    "requestId": "a5b9a631-a971-4bcf-ba65-80c313525a78",
    "result": {
        "data": {
            "@type": "g:List",
            "@value": []
        },
        "meta": {
            "@type": "g:Map",
            "@value": []
        }
    },
    "status": {
        "attributes": {
            "@type": "g:Map",
            "@value": [
                "stackTrace",
                "org.codehaus.groovy.control.MultipleCompilationErrorsException: startup failed:\\nScript18.groovy: 1: [Static type checking] - Cannot find matching method org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal#dropp(). Please check if the declared type is correct and if the method exists.\\n @ line 1, column 1.\\n   g.V().dropp()\\n   ^\\n\\n1 error\\n\\n\\tat org.codehaus.groovy.control.ErrorCollector.failIfErrors(ErrorCollector.java:311)\\n\\tat org.codehaus.groovy.control.CompilationUnit.applyToPrimaryClassNodes(CompilationUnit.java:1102)\\n\\tat org.codehaus.groovy.control.CompilationUnit.doPhaseOperation(CompilationUnit.java:645)\\n\\tat org.codehaus.groovy.control.CompilationUnit.processPhaseOperations(CompilationUnit.java:623)\\n\\tat org.codehaus.groovy.control.CompilationUnit.compile(CompilationUnit.java:600)\\n\\tat groovy.lang.GroovyClassLoader.doParseClass(GroovyClassLoader.java:390)\\n\\tat groovy.lang.GroovyClassLoader.access$300(GroovyClassLoader.java:89)\\n\\tat groovy.lang.GroovyClassLoader$5.provide(GroovyClassLoader.java:330)\\n\\tat groovy.lang.GroovyClassLoader$5.provide(GroovyClassLoader.java:327)\\n\\tat org.codehaus.groovy.runtime.memoize.ConcurrentCommonCache.getAndPut(ConcurrentCommonCache.java:147)\\n\\tat groovy.lang.GroovyClassLoader.parseClass(GroovyClassLoader.java:325)\\n\\tat groovy.lang.GroovyClassLoader.parseClass(GroovyClassLoader.java:309)\\n\\tat groovy.lang.GroovyClassLoader.parseClass(GroovyClassLoader.java:251)\\n\\tat org.apache.tinkerpop.gremlin.groovy.jsr223.GraphDbGremlinGroovyScriptEngine$GroovyCacheLoader.lambda$load$0(GraphDbGremlinGroovyScriptEngine.java:831)\\n\\tat java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1590)\\n\\tat java.util.concurrent.CompletableFuture.asyncSupplyStage(CompletableFuture.java:1604)\\n\\tat java.util.concurrent.CompletableFuture.supplyAsync(CompletableFuture.java:1830)\\n\\tat org.apache.tinkerpop.gremlin.groovy.jsr223.GraphDbGremlinGroovyScriptEngine$GroovyCacheLoader.load(GraphDbGremlinGroovyScriptEngine.java:829)\\n\\tat org.apache.tinkerpop.gremlin.groovy.jsr223.GraphDbGremlinGroovyScriptEngine$GroovyCacheLoader.load(GraphDbGremlinGroovyScriptEngine.java:824)\\n\\tat com.github.benmanes.caffeine.cache.BoundedLocalCache$BoundedLocalLoadingCache.lambda$new$0(BoundedLocalCache.java:3366)\\n\\tat com.github.benmanes.caffeine.cache.LocalCache.lambda$statsAware$0(LocalCache.java:143)\\n\\tat com.github.benmanes.caffeine.cache.BoundedLocalCache.lambda$doComputeIfAbsent$14(BoundedLocalCache.java:2039)\\n\\tat java.util.concurrent.ConcurrentHashMap.compute(ConcurrentHashMap.java:1892)\\n\\tat com.github.benmanes.caffeine.cache.BoundedLocalCache.doComputeIfAbsent(BoundedLocalCache.java:2037)\\n\\tat com.github.benmanes.caffeine.cache.BoundedLocalCache.computeIfAbsent(BoundedLocalCache.java:2020)\\n\\tat com.github.benmanes.caffeine.cache.LocalCache.computeIfAbsent(LocalCache.java:112)\\n\\tat com.github.benmanes.caffeine.cache.LocalLoadingCache.get(LocalLoadingCache.java:67)\\n\\tat org.apache.tinkerpop.gremlin.groovy.jsr223.GraphDbGremlinGroovyScriptEngine.getScriptClass(GraphDbGremlinGroovyScriptEngine.java:573)\\n\\tat org.apache.tinkerpop.gremlin.groovy.jsr223.GraphDbGremlinGroovyScriptEngine.eval(GraphDbGremlinGroovyScriptEngine.java:376)\\n\\tat javax.script.AbstractScriptEngine.eval(AbstractScriptEngine.java:233)\\n\\tat org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor.lambda$eval$0(GremlinExecutor.java:266)\\n\\tat java.util.concurrent.FutureTask.run(FutureTask.java:266)\\n\\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\\n\\tat java.util.concurrent.FutureTask.run(FutureTask.java:266)\\n\\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\\n\\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)\\n\\tat java.lang.Thread.run(Thread.java:748)\\n",
                "exceptions",
                {
                    "@type": "g:List",
                    "@value": [
                        "org.codehaus.groovy.control.MultipleCompilationErrorsException"
                    ]
                }
            ]
        },
        "code": 597,
        "message": "startup failed:\nScript18.groovy: 1: [Static type checking] - Cannot find matching method org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal#dropp(). Please check if the declared type is correct and if the method exists.\n @ line 1, column 1.\n   g.V().dropp()\n   ^\n\n1 error\n"
    }
}
`
	resp_username_error = `
{
    "requestId": "a5b9a631-a971-4bcf-ba65-80c313525a78",
    "result": {
        "data": {
            "@type": "g:List",
            "@value": []
        },
        "meta": {
            "@type": "g:Map",
            "@value": []
        }
    },
    "status": {
        "attributes": {
            "@type": "g:Map",
            "@value": []
        },
        "code": 401,
        "message": "Username and/or password are incorrect"
    }
}
`
)

func TestReadResponse(t *testing.T) {
	Convey("read response", t, func() {

		Convey("read nil response", func() {
			resp, err := ReadResponse(nil)
			So(resp, ShouldBeNil)
			So(err, ShouldBeNil)
		})

		Convey("read 200 count response", func() {
			resp, err := ReadResponse([]byte(resp200_count))
			So(err, ShouldBeNil)
			So(resp, ShouldNotBeNil)

			So(resp.Code, ShouldEqual, 200)
			So(resp.Data, ShouldNotBeNil)
		})

		Convey("read 204 empty response", func() {
			resp, err := ReadResponse([]byte(resp204_empty))
			So(err, ShouldBeNil)
			So(resp, ShouldNotBeNil)

			So(resp.Code, ShouldEqual, 204)
			So(resp.Data, ShouldBeNil)

			ret, err := GetResult(resp)
			So(ret, ShouldBeNil)
			So(err, ShouldBeNil)
		})

		Convey("read failed response", func() {
			resp, err := ReadResponse([]byte(resp_failed))
			So(resp, ShouldBeNil)
			So(err, ShouldNotBeNil)
		})

		Convey("read error response", func() {
			resp, err := ReadResponse([]byte(resp_error))
			So(err, ShouldBeNil)
			So(resp, ShouldNotBeNil)

			So(resp.Code, ShouldEqual, 597)

			ret, err := GetResult(resp)
			So(ret, ShouldBeNil)
			So(err, ShouldNotBeNil)
		})

		Convey("username incorrect response", func() {
            resp, err := ReadResponse([]byte(resp_username_error))
            So(err, ShouldBeNil)
            So(resp, ShouldNotBeNil)

            So(resp.Code, ShouldEqual, 401)

            ret, err := GetResult(resp)
            So(ret, ShouldBeNil)
            So(err, ShouldNotBeNil)
        })
	})
}
