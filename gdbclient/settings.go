/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/11/25
 */

package gdbclient

import (
	"github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient/internal/pool"
	"strconv"
	"time"
)

type Settings struct {
	// port of GDB to connect, default is 8182
	Port int
	// host that the driver will connect to, default is 'localhost'
	Host string
	// username and password for GDB auth
	Username, Password string
	// serializer for the driver of request to GDB and response from
	Serializer string
	// manageTransaction by user client or not in session
	IsManageTransaction bool

	// maximum number of socket connections, Default is 8
	PoolSize int
	// Minimum number of idle connections
	MinIdleConns int
	// Amount of time after which client closes idle connections
	// Default is 5 minutes, -1 disables idle timeout check
	IdleTimeout time.Duration
	// Frequency of idle checks made by idle connections reaper
	// Default is 1 minute. -1 disables idle connections reaper, but idle
	// connections are still discarded by the client if IdleTimeout is set
	IdleCheckFrequency time.Duration
	// Connection age at which client retires (closes) the connection.
	// Default is to not close aged connections.
	MaxConnAge time.Duration
	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	PoolTimeout time.Duration

	// Frequency of WebSocket ping checks, Default is 1 minute
	PingInterval time.Duration
	// max concurrent request pending on one connection, Default is 4
	MaxConcurrentRequest int
	// Amount of time client waits connection io write before returning an error.
	// Default is 5 second
	WriteTimeout time.Duration
	// Amount of time client waits connection io read before returning an error.
	// Default is the same with WriteTimeout
	ReadTimeout time.Duration
}

func (s *Settings) init() {
	if s.Host == "" {
		s.Host = "localhost"
	}
	if s.Port == 0 {
		s.Port = 8182
	}
	if s.PoolSize == 0 {
		s.PoolSize = 8
	}
	if s.IdleTimeout == 0 {
		s.IdleTimeout = 5 * time.Minute
	}
	if s.IdleCheckFrequency == 0 {
		s.IdleCheckFrequency = 1 * time.Minute
	}
	if s.PingInterval == 0 {
		s.PingInterval = 1 * time.Minute
	}
	if s.MaxConcurrentRequest == 0 {
		s.MaxConcurrentRequest = 4
	}
	if s.WriteTimeout == 0 {
		s.WriteTimeout = 5 * time.Second
	}
	if s.ReadTimeout == 0 {
		s.ReadTimeout = s.WriteTimeout
	}
	if s.PoolTimeout == 0 {
		s.PoolTimeout = s.ReadTimeout + 1
	}
}

func (s *Settings) getOpts() *pool.Options {
	return &pool.Options{
		Endpoint:        s.Host + ":" + strconv.FormatInt(int64(s.Port), 10),
		Username:        s.Username,
		Password:        s.Password,
		PingInterval:    s.PingInterval,
		WriteTimeout:    s.WriteTimeout,
		ReadTimeout:     s.ReadTimeout,
		OnGoingRequests: s.MaxConcurrentRequest,

		PoolSize:           s.PoolSize,
		MinIdleConns:       s.MinIdleConns,
		MaxConnAge:         s.MaxConnAge,
		PoolTimeout:        s.PoolTimeout,
		IdleTimeout:        s.IdleTimeout,
		IdleCheckFrequency: s.IdleCheckFrequency,
		Dialer:             pool.NewConnWebSocket,
	}
}

func (s *Settings) getSessionOpts() *pool.Options {
	return &pool.Options{
		Endpoint:        s.Host + ":" + strconv.FormatInt(int64(s.Port), 10),
		Username:        s.Username,
		Password:        s.Password,
		PingInterval:    s.PingInterval,
		WriteTimeout:    s.WriteTimeout,
		ReadTimeout:     s.ReadTimeout,
		OnGoingRequests: 1,

		PoolSize:           1,
		MinIdleConns:       1,
		MaxConnAge:         -1,
		PoolTimeout:        s.PoolTimeout,
		IdleTimeout:        s.PingInterval * 5,
		IdleCheckFrequency: s.PingInterval * 2,
		Dialer:             pool.NewConnWebSocket,
	}
}
