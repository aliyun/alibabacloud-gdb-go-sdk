/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/11/29
 */

package pool

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestNewConnPool(t *testing.T) {
	options := &Options{
		Dialer:          NewConnMock,
		PingInterval:    2 * time.Second,
		WriteTimeout:    1 * time.Second,
		ReadTimeout:     1 * time.Second,
		OnGoingRequests: 16,

		PoolSize:           10,
		PoolTimeout:        time.Hour,
		IdleTimeout:        10 * time.Minute,
		IdleCheckFrequency: time.Minute,
	}

	Convey("create new ConnPool without idle conn", t, func() {

		Convey("connPool get, put and remove should be ok", func() {
			var connPool Pooler = NewConnPool(options)

			conn, err := connPool.Get()
			So(conn, ShouldNotBeNil)
			So(err, ShouldBeNil)

			// get one connect from pool
			So(connPool.Len(), ShouldEqual, 1)

			var conns []Conn
			for i := 0; i < 9; i++ {
				conn, err := connPool.Get()
				So(err, ShouldBeNil)
				conns = append(conns, conn)
			}

			// get all 10 connects from pool
			So(connPool.Len(), ShouldEqual, 10)
			So(connPool.IdleLen(), ShouldEqual, 0)

			connPool.Remove(conn)
			// remove 1 connect from pool
			So(connPool.Len(), ShouldEqual, 9)
			So(connPool.IdleLen(), ShouldEqual, 0)

			// give back connects to pool, it's idle now
			for _, conn := range conns {
				connPool.Put(conn)
			}

			So(connPool.Len(), ShouldEqual, 9)
			So(connPool.IdleLen(), ShouldEqual, 9)

			connPool.Close()
			So(connPool.Len(), ShouldEqual, 0)
			So(connPool.IdleLen(), ShouldEqual, 0)
		})

		Convey("should unblock client when conn is removed", func() {
			defer func() { options.PoolSize = 10 }()
			options.PoolSize = 1

			var connPool Pooler = NewConnPool(options)

			conn, err := connPool.Get()
			So(conn, ShouldNotBeNil)
			So(err, ShouldBeNil)

			started := make(chan bool, 1)
			done := make(chan bool, 1)
			go func() {
				defer func() {
					if err := recover(); err != nil {
						done <- true
						connPool.Put(conn)
					}
				}()

				started <- true
				_, err := connPool.Get()
				So(err, ShouldBeNil)

				done <- true
				connPool.Put(conn)
			}()
			// wait start
			<-started

			// get should be blocked
			select {
			case <-done:
				// fail
				So(1, ShouldEqual, 2)
			case <-time.After(time.Millisecond):
				// ok
			}

			connPool.Remove(conn)

			// here get should be unblocked
			select {
			case val := <-done:
				// ok
				So(val, ShouldEqual, true)
			case <-time.After(time.Second):
				// fail
				So(1, ShouldEqual, 2)
			}

			So(connPool.Len(), ShouldEqual, 1)
			So(connPool.IdleLen(), ShouldEqual, 1)

			connPool.Close()
		})
	})

	Convey("new connPool with min idle connections", t, func() {
		defer func() {
			options.PoolTimeout = time.Hour
			options.IdleTimeout = 10 * time.Minute
			options.IdleCheckFrequency = time.Minute
			options.PoolSize = 10
			options.MinIdleConns = 0
		}()

		options.PoolSize = 16
		options.PoolTimeout = 100 * time.Second
		options.IdleTimeout = -1
		options.IdleCheckFrequency = -1
		options.MinIdleConns = 1

		var connPool Pooler = NewConnPool(options)
		// has idle connections when created
		for {
			if connPool.Len() != options.MinIdleConns {
				time.Sleep(time.Millisecond)
			} else {
				break
			}
		}

		Convey("idle connections after get, remove", func() {
			So(connPool.IdleLen(), ShouldEqual, options.MinIdleConns)
			So(connPool.Len(), ShouldEqual, options.MinIdleConns)

			// idle connections after get
			cn, err := connPool.Get()
			So(cn, ShouldNotBeNil)
			So(err, ShouldBeNil)

			// wait idle create
			for {
				if connPool.Len() != options.MinIdleConns+1 {
					time.Sleep(time.Millisecond)
				} else {
					break
				}
			}
			So(connPool.IdleLen(), ShouldEqual, options.MinIdleConns)
			So(connPool.Len(), ShouldEqual, options.MinIdleConns+1)

			// idle connections after remove
			connPool.Remove(cn)
			So(connPool.IdleLen(), ShouldEqual, options.MinIdleConns)
			So(connPool.Len(), ShouldEqual, options.MinIdleConns)
		})

		Convey("idle connections not exceed pool size", func() {
			var conns []Conn

			for i := 0; i < options.PoolSize; i++ {
				cn, err := connPool.Get()
				So(cn, ShouldNotBeNil)
				So(err, ShouldBeNil)

				// get connect from pool
				if !cn.Pooled() {
					time.Sleep(5 * time.Millisecond)
					i--
				}
				conns = append(conns, cn)
			}
			So(connPool.Len(), ShouldEqual, options.PoolSize)
			So(connPool.IdleLen(), ShouldEqual, 0)

			// connections include pooled or un-pooled
			So(len(conns), ShouldBeGreaterThanOrEqualTo, options.PoolSize)

			// put all connections to pool
			for _, conn := range conns {
				connPool.Put(conn)
			}
			So(connPool.Len(), ShouldEqual, options.PoolSize)
			So(connPool.IdleLen(), ShouldEqual, options.PoolSize)
		})

		connPool.Close()
	})

}
