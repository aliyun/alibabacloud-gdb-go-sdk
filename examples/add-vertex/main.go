/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/11/22
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

	// send script dsl to GDB
	dsl := "g.addV('goTest').property(id, '12').property('name', 'Luck')"
	results, err := client.SubmitScript(dsl)
	if err != nil {
		log.Fatalf("Error while querying: %s\n", err.Error())
	}

	// drop all vertex
	_, err = client.SubmitScript("g.V().drop()")
	if err != nil {
		log.Fatalf("Error while querying: %s\n", err.Error())
	}

	// get response, add vertex should return a Vertex
	for _, result := range results {
		v := result.GetVertex()
		log.Printf("get vertex: %s", v.String())

		// read vertex property
		for _, p := range v.Properties() {
			log.Printf("prop: %s", p.String())
		}
	}

	// send script dsl with bindings to GDB
	bindings := make(map[string]interface{})
	bindings["GDB___id"] = "22"
	bindings["GDB___label"] = "goTest"
	bindings["GDB___PK"] = "name"
	bindings["GDB___PV"] = "Jack"

	dsl = "g.addV(GDB___label).property(id, GDB___id).property(GDB___PK, GDB___PV)"
	results, err = client.SubmitScriptBound(dsl, bindings)
	if err != nil {
		log.Fatalf("Error while querying: %s\n", err.Error())
	}

	// get response, add vertex should return a Vertex
	for _, result := range results {
		v := result.GetVertex()
		log.Printf("get vertex: %s", v.String())

		// read vertex property
		for _, p := range v.VProperties() {
			log.Printf("prop: %s", p.String())
		}
	}

	// drop all vertex
	_, err = client.SubmitScript("g.V().drop()")
	if err != nil {
		log.Fatalf("Error while querying: %s\n", err.Error())
	}

	client.Close()
}
