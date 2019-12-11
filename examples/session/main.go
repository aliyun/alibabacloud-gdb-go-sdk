/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

/**
 * @author : Liu Jianping
 * @date : 2019/12/4
 */

package main

import (
	"flag"
	"github.com/google/uuid"
	"log"
	"time"

	goClient "github.com/aliyun/alibabacloud-gdb-go-client/gdbclient"
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
	sessionId, _ := uuid.NewUUID()
	client := goClient.NewSessionClient(sessionId.String(), settings)
	time.Sleep(1 * time.Second)

	client.BatchSubmit(func(c goClient.ClientShell) error {
		bindings := make(map[string]interface{})
		bindings["GDB___label"] = "goTest"
		bindings["GDB___PK1"] = "name"
		bindings["GDB___PK2"] = "age"
		dsl := "g.addV(GDB___label).property(id, GDB___id).property(GDB___PK1, GDB___PV1).property(GDB___PK2, GDB___PV2)"

		bindings["GDB___id"] = "100"
		bindings["GDB___PV1"] = "Jack"
		bindings["GDB___PV2"] = 32

		log.Printf("add vertex[id:%s]", bindings["GDB___id"].(string))
		_, err := c.SubmitScriptBound(dsl, bindings)
		if err != nil {
			return err
		}

		bindings["GDB___id"] = "101"
		bindings["GDB___PV1"] = "Luck"
		bindings["GDB___PV2"] = 34

		log.Printf("add vertex[id:%s]", bindings["GDB___id"].(string))
		_, err = c.SubmitScriptBound(dsl, bindings)
		if err != nil {
			return err
		}

		bindings["GDB___id"] = "102"
		bindings["GDB___PV1"] = "John"
		bindings["GDB___PV2"] = 22

		log.Printf("add vertex[id:%s]", bindings["GDB___id"].(string))
		_, err = c.SubmitScriptBound(dsl, bindings)
		if err != nil {
			return err
		}

		dsl = "g.addE(GDB___label).from(__.V(GDB___from)).to(__.V(GDB___to)).property(id, GDB___id).property(GDB___PK1, GDB___PV1)"

		bindings["GDB___id"] = "201"
		bindings["GDB___PV1"] = "created"
		bindings["GDB___from"] = "100"
		bindings["GDB___to"] = "101"

		log.Printf("add edge[id:%s]", bindings["GDB___id"].(string))
		_, err = c.SubmitScriptBound(dsl, bindings)
		if err != nil {
			return err
		}

		bindings["GDB___id"] = "202"
		bindings["GDB___PV1"] = "created"
		bindings["GDB___from"] = "100"
		bindings["GDB___to"] = "102"

		log.Printf("add edge[id:%s]", bindings["GDB___id"].(string))
		_, err = c.SubmitScriptBound(dsl, bindings)
		if err != nil {
			return err
		}

		return nil
	})

	client.BatchSubmit(func(c goClient.ClientShell) error {
		results, err := c.SubmitScript("g.V().id()")
		if err != nil {
			return err
		}
		for _, r := range results {
			log.Print("v: " + r.GetString())
		}

		results, err = c.SubmitScript("g.E().id()")
		if err != nil {
			return err
		}
		for _, r := range results {
			log.Print("e: " + r.GetString())
		}

		return nil
	})

	client.BatchSubmit(func(c goClient.ClientShell) error {
		_, err := c.SubmitScript("g.V().hasLabel('goTest').drop()")
		return err
	})

	client.Close()
}
