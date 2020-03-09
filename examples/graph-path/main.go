/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2020/3/9
 */

package main

import (
	"flag"
	"log"

	goClient "github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient"
)

var (
	host, username, password string
	port                     int
)

func createModernGraphData(client goClient.Client) {
	dslList := []string{
		"g.addV('person').property(id, 'marko').property('age', 28).property('name', 'marko')",
		"g.addV('person').property(id, 'vadas').property('age', 27).property('name', 'vadas')",
		"g.addV('person').property(id, 'josh').property('age', 32).property('name', 'josh')",
		"g.addV('person').property(id, 'Peter').property('age', 35).property('name', 'Peter')",
		"g.addV('software').property(id, 'lop').property('lang', 'java').property('name', 'lop')",
		"g.addV('software').property(id, 'ripple').property('lang', 'java').property('name', 'ripple')",
		"g.addE('knows').from(V('marko')).to(V('vadas')).property('weight', 0.5f)",
		"g.addE('knows').from(V('marko')).to(V('josh')).property('weight', 1.0f)",
		"g.addE('created').from(V('marko')).to(V('lop')).property('weight', 0.4f)",
		"g.addE('created').from(V('josh')).to(V('lop')).property('weight', 0.4f)",
		"g.addE('created').from(V('josh')).to(V('ripple')).property('weight', 1.0f)",
		"g.addE('created').from(V('Peter')).to(V('lop')).property('weight', 0.2f)",
	}

	for _, dsl := range dslList {
		_, err := client.SubmitScript(dsl)
		if err != nil {
			log.Printf("dsl: %s, err: %s", dsl, err.Error())
		}
	}
}

func main() {
	flag.StringVar(&host, "host", "", "GDB Connection Host")
	flag.StringVar(&username, "username", "", "GDB username")
	flag.StringVar(&password, "password", "", "GDB password")
	flag.IntVar(&port, "port", 8182, "GDB Connection Port")
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
	}

	// connect GDB with auth
	client := goClient.NewClient(settings)

	// add basic data
	createModernGraphData(client)

	// send script dsl to GDB
	bindings := make(map[string]interface{})
	bindings["GDB___id"] = "marko"
	dsl := "g.V(GDB___id).repeat(out()).emit().times(3).path()"
	results, err := client.SubmitScriptBound(dsl, bindings)
	if err != nil {
		log.Fatalf("Error while querying: %s\n", err.Error())
	}

	// get response
	for _, result := range results {
		log.Println(result.GetPath())
	}

	client.Close()
}
