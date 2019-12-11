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

	_, err := client.SubmitScript("g.V().dropp()")

	log.Printf("dsl error : %s", err.Error())

	client.Close()
}
