/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/12/5
 */

package main

import (
	"flag"
	"github.com/google/uuid"
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
	port, runningTime        int

	ThreadCnt = 32
	BatchSize = 32
)

func batchAddVertex(client goClient.SessionClient) error {
	bindings := make(map[string]interface{})
	dsl := "g.addV('goTest').property(id, GDB___id).property('name', GDB___PV).id()"

	err := client.BatchSubmit(func(shell goClient.ClientShell) error {
		for i := 0; i < BatchSize; i++ {
			id, _ := uuid.NewUUID()
			bindings["GDB___id"] = id.String()
			bindings["GDB___PV"] = rand.Int63()
			_, err := shell.SubmitScriptBound(dsl, bindings)
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func initLogger() *zap.Logger {
	file, _ := os.Create("/tmp/test.log")
	writeSyncer := zapcore.AddSync(file)
	encoder := zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(encoder, writeSyncer, zapcore.ErrorLevel)

	return zap.New(core, zap.AddCaller())
}

func main() {
	flag.StringVar(&host, "host", "", "GDB Connection Host")
	flag.StringVar(&username, "username", "", "GDB username")
	flag.StringVar(&password, "password", "", "GDB password")
	flag.IntVar(&port, "port", 8182, "GDB Connection Port")
	flag.IntVar(&runningTime, "time", 500, "bench running time(s)")
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

		PingInterval: time.Minute,
		WriteTimeout: 2 * time.Second,
	}

	// set log
	goClient.SetLogger(initLogger())

	var wg sync.WaitGroup
	var quit chan struct{}
	rand.Seed(time.Now().UnixNano())
	wg.Add(ThreadCnt)
	quit = make(chan struct{}, 1)

	for i := 0; i < ThreadCnt; i++ {
		go func() {
			// connect GDB with auth
			sessionId, _ := uuid.NewUUID()
			client := goClient.NewSessionClient(sessionId.String(), settings)
			time.Sleep(1 * time.Second)

			defer func() {
				client.Close()
				wg.Done()
			}()

			for {
				select {
				case <-quit:
					return
				default:
				}

				err := batchAddVertex(client)
				if err != nil {
					log.Printf("error : %s", err.Error())
					return
				}
			}
		}()
	}

	endTime := time.Now().Add(time.Second * time.Duration(runningTime))
	client := goClient.NewClient(settings)

	total := int64(0)
	ticker := time.NewTicker(2 * time.Second)

Loop:
	for {
		select {
		case <-ticker.C:
			if results, err := client.SubmitScript("g.V().count()"); err == nil {
				count := results[0].GetInt64()
				log.Printf("total %d, %f qps", count, float64(count-total)/2.0)
				total = count
			} else {
				break Loop
			}

			if time.Now().After(endTime) {
				break Loop
			}
		}
	}

	client.Close()
	ticker.Stop()
	quit <- struct{}{}
	close(quit)
	wg.Wait()

	log.Printf("Byebye...")
}
