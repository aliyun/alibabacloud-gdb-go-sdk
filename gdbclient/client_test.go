/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2020/3/23
 */

package gdbclient

import (
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal/pool"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	server := pool.StartGdbTestServer()
	defer server.CloseGdbTestServer()

	// set sdk in test mode
	os.Setenv("GO_CLIENT_TEST_URL", server.WsUrl)

	settings := &Settings{
		Host: "127.0.0.1",
		Port: 8182,

		PoolSize:             4,
		MaxConcurrentRequest: 4,

		PingInterval:       20 * time.Second,
		AliveCheckInterval: 1 * time.Minute,
		PoolTimeout:        200 * time.Millisecond,
		WriteTimeout:       200 * time.Millisecond,
	}

	Convey("create new client", t, func() {
		client := NewClient(settings)

		results, err := client.SubmitScript("g.V().count")
		So(err, ShouldBeNil)
		So(results, ShouldNotBeNil)

		So(results[0].GetInt64(), ShouldEqual, 0)

		client.Close()

		_, err = client.SubmitScript("g.V().count")
		So(err.Error(), ShouldContainSubstring, "pool closed")
	})

	Convey("send multi request and pending", t, func() {
		// make a delay during process on server
		orgFunc := server.WsMakeResponseFunc
		server.WsMakeResponseFunc = func(requestId string) []byte {
			time.Sleep(100 * time.Millisecond)
			return orgFunc(requestId)
		}
		settings.PoolTimeout = 20 * time.Millisecond

		Convey("multi request and waiting in one routine", func() {
			client := NewClient(settings)

			var futureList []ResultSetFuture
			for i := 0; i < settings.MaxConcurrentRequest*settings.PoolSize; i++ {
				f, err := client.SubmitScriptAsync("g.V().count()")
				So(err, ShouldBeNil)

				futureList = append(futureList, f)
			}

			_, err := client.SubmitScriptAsync("g.V().count()")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "get connection timeout")

			// sleep a while to wait request complete(100ms delay in server)
			time.Sleep(70 * time.Millisecond)

			f, err := client.SubmitScriptAsync("g.V().count()")
			So(err, ShouldBeNil)

			for _, f := range futureList {
				r, err := f.GetResults()
				So(err, ShouldBeNil)
				So(r[0].GetInt64(), ShouldEqual, 0)
			}

			_, err = f.GetResults()
			So(err, ShouldBeNil)

			client.Close()
		})

		Convey("multi request and waiting in numbers of routine as poolSize", func(c C) {
			client := NewClient(settings)
			wg := sync.WaitGroup{}
			quit := make(chan struct{})
			wg.Add(settings.PoolSize)

			// send request in multi routine
			for i := 0; i < settings.PoolSize; i++ {
				go func() {
					defer wg.Done()

					for {
						var futureList []ResultSetFuture

						for k := 0; k < settings.MaxConcurrentRequest; k++ {
							f, err := client.SubmitScriptAsync("g.V().count()")
							c.So(err, ShouldBeNil)

							futureList = append(futureList, f)
						}

						for _, f := range futureList {
							_, err := f.GetResults()
							c.So(err, ShouldBeNil)
						}

						select {
						case <-quit:
							return
						default:
						}
					}
				}()
			}

			// go routine is flying, and request is pending
			time.Sleep(50 * time.Millisecond)

			_, err := client.SubmitScriptAsync("g.V().count()")
			So(err, ShouldNotBeNil)

			// wait submit routines to exit
			close(quit)
			wg.Wait()

			_, err = client.SubmitScriptAsync("g.V().count()")
			So(err, ShouldBeNil)

			client.Close()
		})
	})
}

func TestNewSessionClient(t *testing.T) {
	settings := &Settings{
		Host:     "127.0.0.1",
		Port:     8182,
		Username: "zhiyan",
		Password: "zhiyan",

		PoolSize:             4,
		MaxConcurrentRequest: 4,

		PingInterval:       20 * time.Second,
		AliveCheckInterval: 1 * time.Minute,
		PoolTimeout:        500 * time.Millisecond,
		WriteTimeout:       200 * time.Millisecond,
	}

	SkipConvey("create new session client and batch submit", t, func() {
		client := NewSessionClient("uuid-unique-string", settings)

		client.BatchSubmit(func(shell ClientShell) error {
			results, err := shell.SubmitScript("g.addV('testV').property('name', 'Luck')")
			So(err, ShouldBeNil)
			id1 := results[0].GetVertex().Id()

			results, err = shell.SubmitScript("g.addV('testV').property('name', 'Jack')")
			So(err, ShouldBeNil)
			id2 := results[0].GetVertex().Id()

			bindings := make(map[string]interface{})
			bindings["GDB___to"] = id1
			bindings["GDB___from"] = id2
			dsl := "g.addE('testE').to(V(GDB___to)).from(V(GDB___from)).property('connect', 'friend')"
			_, err = shell.SubmitScriptBound(dsl, bindings)
			So(err, ShouldBeNil)

			return nil
		})

		client.Close()
	})

	SkipConvey("create new session client and fail to submit", t, func() {
		client := NewSessionClient("uuid-unique-string", settings)

		var addVid string

		// add vertices with the same id
		client.BatchSubmit(func(shell ClientShell) error {
			results, err := shell.SubmitScript("g.addV('testV').property('name', 'Luck')")
			So(err, ShouldBeNil)
			addVid = results[0].GetVertex().Id()

			bindings := make(map[string]interface{})
			bindings["GDB___id"] = addVid
			dsl := "g.addV('testV').property(id, GDB___id).property('name', 'Jack')"
			_, err = shell.SubmitScriptBound(dsl, bindings)
			So(err, ShouldNotBeNil)

			return err
		})

		// rollback the add vertex request batch, so no vertex 'addVid' in db

		client.BatchSubmit(func(shell ClientShell) error {

			bindings := make(map[string]interface{})
			bindings["GDB___id"] = addVid
			results, err := shell.SubmitScriptBound("g.V(GDB___id).count()", bindings)
			So(err, ShouldBeNil)
			So(results[0].GetInt64(), ShouldEqual, 0)

			return nil
		})

		client.Close()
	})
}
