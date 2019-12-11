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

package internal

import "go.uber.org/zap"

//var Logger = log.New(os.Stderr, "Gdb: ", log.LstdFlags|log.Lshortfile)

var Logger = zap.NewExample(zap.AddCaller(), zap.Development())
