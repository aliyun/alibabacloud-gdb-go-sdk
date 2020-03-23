/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2020/3/1
 */

package pool

import (
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewConnPool(t *testing.T) {
	server := StartGdbTestServer()
	defer server.CloseGdbTestServer()

	var options = &Options{
		Dialer:                      NewConnWebSocket,
		GdbUrl:                      server.WsUrl,
		PingInterval:                2 * time.Second,
		WriteTimeout:                1 * time.Second,
		ReadTimeout:                 1 * time.Second,
		MaxInProcessPerConn:         4,
		MaxSimultaneousUsagePerConn: 4,

		PoolSize:           4,
		PoolTimeout:        2 * time.Second,
		AliveCheckInterval: 5 * time.Second,
	}

	Convey("create and close", t, func() {
		pool := NewConnPool(options)
		So(pool, ShouldNotBeNil)
		So(pool.closed(), ShouldBeFalse)

		for {
			if pool.Size() < options.PoolSize {
				time.Sleep(time.Millisecond)
			} else {
				break
			}
		}
		conn, err := pool.Get()
		So(err, ShouldBeNil)
		So(conn.borrowed, ShouldEqual, 1)

		So(pool.Size(), ShouldEqual, options.PoolSize)
		So(pool.dialErrorsNum, ShouldEqual, 0)

		pool.Close()
		So(pool.closed(), ShouldBeTrue)
		So(pool.Size(), ShouldEqual, 0)

		// connection in pool should be closed
		So(conn.closed(), ShouldBeTrue)
	})

	Convey("get and put", t, func() {
		pool := NewConnPool(options)
		So(pool, ShouldNotBeNil)
		So(pool.closed(), ShouldBeFalse)

		conn, err := pool.Get()
		So(err, ShouldBeNil)
		So(conn.borrowed, ShouldEqual, 1)

		So(conn.closed(), ShouldBeFalse)
		pool.Put(conn)

		pool.Close()
		So(pool.Size(), ShouldEqual, 0)
	})

	Convey("get and put multi", t, func() {
		pool := NewConnPool(options)
		So(pool, ShouldNotBeNil)

		var connList []*ConnWebSocket
		for i := 0; i < options.PoolSize*options.MaxSimultaneousUsagePerConn; i++ {
			conn, err := pool.Get()
			So(err, ShouldBeNil)

			// set borrowed directly in mock
			atomic.StoreInt32(&conn.pendingSize, int32(options.MaxInProcessPerConn))
			connList = append(connList, conn)
		}

		done := make(chan bool, 1)
		go func() {
			done <- true
			pool.Put(connList[0])
		}()

		<-done
		conn, err := pool.Get()
		So(err, ShouldBeNil)
		So(conn.borrowed, ShouldEqual, options.MaxSimultaneousUsagePerConn)

		pool.Close()
	})
}

func TestConnPoolBroken(t *testing.T) {
	server := StartGdbTestServer()
	defer server.CloseGdbTestServer()

	var options = &Options{
		Dialer:                      NewConnWebSocket,
		GdbUrl:                      server.WsUrl,
		PingInterval:                200 * time.Millisecond,
		WriteTimeout:                200 * time.Millisecond,
		ReadTimeout:                 200 * time.Millisecond,
		MaxInProcessPerConn:         4,
		MaxSimultaneousUsagePerConn: 4,

		PoolSize:           4,
		PoolTimeout:        200 * time.Millisecond,
		AliveCheckInterval: 1 * time.Second,
	}

	Convey("get connection and close", t, func() {
		pool := NewConnPool(options)
		So(pool, ShouldNotBeNil)
		So(pool.closed(), ShouldBeFalse)

		for {
			if pool.Size() < options.PoolSize {
				time.Sleep(time.Millisecond)
			} else {
				break
			}
		}
		// connection pool is finished
		So(atomic.LoadInt32(&pool._opening), ShouldEqual, 0)

		conn, err := pool.Get()
		So(err, ShouldBeNil)
		So(conn.closed(), ShouldBeFalse)

		// close this conn and return to pool
		conn.Close()
		pool.Put(conn)

		// pool remove the broken connection
		So(pool.Size(), ShouldEqual, options.PoolSize-1)

		// pool is creating new connection for replace
		for {
			if pool.Size() < options.PoolSize {
				time.Sleep(time.Millisecond)
			} else {
				break
			}
		}

		So(pool.Size(), ShouldEqual, options.PoolSize)
		pool.Close()
	})

	Convey("close connection and wait recover", t, func() {
		pool := NewConnPool(options)
		So(pool, ShouldNotBeNil)
		So(pool.closed(), ShouldBeFalse)

		for {
			if pool.Size() < options.PoolSize {
				time.Sleep(time.Millisecond)
			} else {
				break
			}
		}

		for _, cn := range pool.conns {
			cn.Close()
		}

		// fail to get a connection as all broken
		_, err := pool.Get()
		So(err.Error(), ShouldEqual, errGetConnTimeout.Error())

		// alive check 1 sec, let pool create connection
		time.Sleep(1200 * time.Millisecond)

		_, err = pool.Get()
		So(err, ShouldBeNil)
		pool.Close()
	})

	Convey("close connection and tick to recover", t, func() {
		pool := NewConnPool(options)
		So(pool, ShouldNotBeNil)
		So(pool.closed(), ShouldBeFalse)

		for {
			if pool.Size() < options.PoolSize {
				time.Sleep(time.Millisecond)
			} else {
				break
			}
		}

		// all connections broken
		for _, cn := range pool.conns {
			cn.Close()
		}

		// fail to get one
		_, err := pool.Get()
		So(err.Error(), ShouldEqual, errGetConnTimeout.Error())

		// tick to recover
		pool.poolNotifier()

		_, err = pool.Get()
		So(err, ShouldBeNil)

		pool.Close()
	})
}
