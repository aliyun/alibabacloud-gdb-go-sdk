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

	// drop all vertex
	_, err := client.SubmitScript("g.V().drop()")
	if err != nil {
		log.Fatalf("Error while querying: %s\n", err.Error())
	}

	// script dsl to add a vertex[12]
	dsl := "g.addV('goTest').property(id, '12').property('name', 'Luck')"
	_, err = client.SubmitScript(dsl)
	if err != nil {
		log.Fatalf("Error while querying: %s\n", err.Error())
	}

	// script dsl to add a vertex[12]
	dsl = "g.addV('goTest').property(id, '22').property('name', 'Jack')"
	_, err = client.SubmitScript(dsl)
	if err != nil {
		log.Fatalf("Error while querying: %s\n", err.Error())
	}

	// send edge added script dsl with bindings to GDB
	bindings := make(map[string]interface{})
	bindings["GDB___id"] = "32"
	bindings["GDB___label"] = "goTestEdge"
	bindings["GDB___from"] = "12"
	bindings["GDB___to"] = "22"
	bindings["GDB___PK"] = "weight"
	bindings["GDB___PV"] = float64(0.85)

	dsl = "g.addE(GDB___label).from(__.V(GDB___from)).to(__.V(GDB___to)).property(id, GDB___id).property(GDB___PK, GDB___PV)"
	results, err := client.SubmitScriptBound(dsl, bindings)
	if err != nil {
		log.Fatalf("Error while querying: %s\n", err.Error())
	}

	for _, result := range results {
		e := result.GetEdge()
		log.Printf("get edge: %s", e.String())

		// read vertex property
		for _, p := range e.Properties() {
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
