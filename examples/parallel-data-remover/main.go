/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2020/3/11
 */

//
// This is a data remover tool for GDB.
// You could remove elements with specified label, or all edges, even more all data.
// If target to remove all data, you'd better remove edges at first in case errors
//

package main

import (
	"flag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	goClient "github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient"
)

var (
	host, username, password string
	port                     int
	edge                     bool
	label                    string
)

func banner(client goClient.Client, edge bool, label string) bool {
	dsl := "g."
	// send edge added script dsl with bindings to GDB
	bindings := make(map[string]interface{})

	tips := "Start to remove all "
	if edge {
		dsl = dsl + "E()."
		tips = tips + "edges"
	} else {
		dsl = dsl + "V()."
		tips = tips + "vertices"
	}
	if label != "" {
		dsl = dsl + "hasLabel(GDB___label)."
		bindings["GDB___label"] = label
		tips = tips + " with label " + label
	}
	log.Println(tips)

	dsl = dsl + "count()"
	results, err := client.SubmitScriptBound(dsl, bindings)
	if err != nil {
		log.Printf("fetch element count failed: %v", err)
		return false
	}

	count := results[0].GetInt64()
	if count > 0 {
		log.Printf("total cnt: " + strconv.FormatInt(count, 10) + ", begin to drop\n")
		return true
	}

	log.Println("total cnt: 0, no need to drop")
	return false
}

func reportCount(client goClient.Client, edge bool, quit chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var orgCount int64 = 0
	dsl := "g.V().count()"
	if edge {
		dsl = "g.E().count()"
	}

	for {
		select {
		case <-ticker.C:
			if results, err := client.SubmitScript(dsl); err == nil {
				currentCount := results[0].GetInt64()
				log.Printf("total %d, %f qps", currentCount, float64(orgCount-currentCount))
				orgCount = currentCount
			} else {
				log.Printf("get count failed: %v", err)
			}
		case <-quit:
			return
		}
	}
}

func dropElementByIdAsync(client goClient.Client, edge bool, ids []string) goClient.ResultSetFuture {
	dsl := "g.V("
	if edge {
		dsl = "g.E("
	}

	bindings := make(map[string]interface{})
	for i, id := range ids {
		tag := "GDB___id" + strconv.Itoa(i)
		if i == 0 {
			dsl = dsl + tag
		} else {
			dsl = dsl + " ," + tag
		}
		bindings[tag] = id
	}
	dsl += ").drop()"

	future, err := client.SubmitScriptBoundAsync(dsl, bindings)
	if err != nil {
		log.Printf("submit request failed: %v", err)
		return nil
	}
	return future
}

func dropElementByLabel(client goClient.Client, edge bool, label string) {
	bindings := make(map[string]interface{})
	bindings["GDB___id"] = ""
	bindings["GDB___label"] = label

	dsl := "g.V()"
	if edge {
		dsl = "g.E()"
	}
	dsl += ".hasLabel(GDB___label).has(id, gt(GDB___id)).limit(2560).id()"

	for {
		results, err := client.SubmitScriptBound(dsl, bindings)
		if err != nil {
			log.Printf("fetch ids failed, try again ? %v", err)
			continue
		}

		var ids []string
		for _, result := range results {
			ids = append(ids, result.GetString())
		}

		if len(ids) == 0 {
			log.Printf("finished label : " + label)
			return
		}

		var reqFuture []goClient.ResultSetFuture
		for i := 0; i < len(ids); i += 64 {
			high := int(math.Min(float64(len(ids)), float64(i+64)))
			reqIds := ids[i:high]

			future := dropElementByIdAsync(client, edge, reqIds)
			if future != nil {
				reqFuture = append(reqFuture, future)
			}
		}

		for _, f := range reqFuture {
			_, err := f.GetResults()
			if err != nil {
				log.Printf("drop failed: %v", err)
			}
		}

		bindings["GDB___id"] = ids[len(ids)-1]
	}
}

func initLogger() *zap.Logger {
	file, _ := os.Create("/tmp/test.log")
	writeSyncer := zapcore.AddSync(file)
	encoder := zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(encoder, writeSyncer, zapcore.DebugLevel)

	return zap.New(core, zap.AddCaller())
}

func main() {
	flag.StringVar(&host, "host", "", "GDB Connection Host")
	flag.StringVar(&username, "username", "", "GDB username")
	flag.StringVar(&password, "password", "", "GDB password")
	flag.IntVar(&port, "port", 8182, "GDB Connection Port")
	flag.BoolVar(&edge, "edge", false, "remove edge only")
	flag.StringVar(&label, "label", "", "drop element with specified label")
	flag.Parse()

	if host == "" || username == "" || password == "" {
		log.Fatal("No enough args provided. Please run:" +
			" go run main.go -host <gdb host> -username <username> -password <password> -port <gdb port>")
		return
	}

	dslPrefix := "g.V()"
	if edge {
		dslPrefix = "g.E()"
	}

	settings := &goClient.Settings{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,

		MaxConcurrentRequest: 64,
	}

	// set log
	goClient.SetLogger(initLogger())

	// connect GDB with auth
	client := goClient.NewClient(settings)
	defer client.Close()

	if ret := banner(client, edge, label); !ret {
		return
	}

	// fetch labels of elements to drop
	var labels []string
	if label == "" {
		fetchLabelsDsl := dslPrefix + ".group().by(label()).select(keys)"
		results, err := client.SubmitScript(fetchLabelsDsl)
		if err != nil {
			log.Printf("fetch all labels failed, please set specified label : %v", err)
			return
		}

		for _, result := range results {
			for _, id := range result.GetList() {
				if idStr, ok := id.(string); ok {
					labels = append(labels, idStr)
				}
			}
		}
	} else {
		labels = append(labels, label)
	}

	// setup monitor
	quit := make(chan struct{})
	go reportCount(client, edge, quit)

	// drop by label
	log.Printf("drop element by labels: %v", labels)
	for _, label = range labels {
		dropElementByLabel(client, edge, label)
	}

	quit <- struct{}{}
	close(quit)
	log.Printf("Byebye...")
}
