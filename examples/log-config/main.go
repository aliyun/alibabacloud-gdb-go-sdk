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
    goClient "github.com/aliyun/alibabacloud-gdb-go-sdk/gdbclient"
    "github.com/natefinch/lumberjack"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "log"
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

    //--------- config log settings -----------//
    // writer
    lumber := &lumberjack.Logger{
        Filename: "/tmp/test.log",
        MaxSize: 10,
        MaxBackups: 5,
        MaxAge: 30,
        Compress: false,
    }
    zapcore.AddSync(lumber)

    // encoder
    encoder := zap.NewProductionEncoderConfig()
    encoder.EncodeTime = zapcore.ISO8601TimeEncoder
    encoder.EncodeLevel = zapcore.CapitalLevelEncoder
    zapcore.NewConsoleEncoder(encoder)

    // core
    core := zapcore.NewCore(zapcore.NewConsoleEncoder(encoder), zapcore.AddSync(lumber), zapcore.DebugLevel)

    // logger
    logger := zap.New(core, zap.AddCaller())

    goClient.SetLogger(logger)
    //--------- config log settings -----------//

    // connect GDB with auth
    client := goClient.NewClient(settings)


    results, err := client.SubmitScript("g.V().count()")
    if err != nil {
        log.Printf(" error : %s", err.Error())
    }
    log.Printf("vertex count : %d", results[0].GetInt64())


    results, err = client.SubmitScript("g.E().count()")
    if err != nil {
        log.Printf(" error : %s", err.Error())
    }
    log.Printf("vertex count : %d", results[0].GetInt64())

    client.Close()
}

