/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2020/3/5
 */

package main

import (
	"flag"
	"github.com/google/uuid"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	goClient "github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient"
)

var (
	host, username, password string
	port                     int
	threadCnt                int
	runningTime              int
)

func initLogger() *zap.Logger {
	file, _ := os.Create("/tmp/test.log")
	writeSyncer := zapcore.AddSync(file)
	encoder := zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(encoder, writeSyncer, zapcore.DebugLevel)

	return zap.New(core, zap.AddCaller())
}

func addVertex(client goClient.Client) error {
	bindings := make(map[string]interface{})

	id, _ := uuid.NewUUID()
	bindings["GDB___id"] = id.String()
	bindings["GDB___PV"] = rand.Int63()
	dsl := "g.addV('goTest').property(id, GDB___id).property('name', GDB___PV)"

	_, err := client.SubmitScriptBound(dsl, bindings)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	flag.StringVar(&host, "host", "", "GDB Connection Host")
	flag.StringVar(&username, "username", "", "GDB username")
	flag.StringVar(&password, "password", "", "GDB password")
	flag.IntVar(&port, "port", 8182, "GDB Connection Port")
	flag.IntVar(&threadCnt, "threadCount", 32, "parallel thread count, default 32")
	flag.IntVar(&runningTime, "runningTime", 300, "running time(ms), default 300")
	flag.Parse()

	if host == "" || username == "" || password == "" {
		log.Fatal("No enough args provided. Please run:" +
			" go run main.go -host <gdb host> -username <username> -password <password> -port <gdb port>")
		return
	}
	settings := &goClient.Settings{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,

		PoolSize:     threadCnt,
		PingInterval: time.Minute,
		WriteTimeout: 2 * time.Second,
	}

	// set log
	goClient.SetLogger(initLogger())
	client := goClient.NewClient(settings)

	// stats
	var wg sync.WaitGroup
	var addCount, failCount atomic.Int64
	quit := make(chan struct{}, 1)
	wg.Add(threadCnt)

	for i := 0; i < threadCnt; i++ {
		go func() {
			for {
				if err := addVertex(client); err != nil {
					failCount.Inc()
				} else {
					addCount.Inc()
				}

				select {
				case <-quit:
					wg.Done()
					return
				default:
				}
			}
		}()
	}

	var currentCount int64 = 0
	ticker := time.NewTicker(2 * time.Second)
	endTime := time.Now().Add(time.Second * time.Duration(runningTime))

Loop:
	for {
		select {
		case <-ticker.C:
			add, fail := addCount.Load(), failCount.Load()
			log.Printf("add %d, fail %d, %f qps", add, fail, float64(add-currentCount)/2.0)
			currentCount = add

			if time.Now().After(endTime) {
				break Loop
			}
		}
	}

	ticker.Stop()
	quit <- struct{}{}
	close(quit)
	wg.Wait()

	// wait routine finished and close client
	client.Close()
	log.Printf("Byebye...")
}
