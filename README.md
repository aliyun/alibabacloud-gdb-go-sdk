# Gremlin Go SDK for GDB

GDB Go语言客户端，参考Gremlin Java开源客户端[gremlin-driver](https://github.com/apache/tinkerpop)，
基于`GraphSonV3d0`序列化协议实现，目前支持同步/异步发送查询script以及返回值解析，支持连接池和session模式。


## Install Go

```
# download pkg
curl -o golang.pkg https://dl.google.com/go/go1.13.3.darwin-amd64.pkg

# install
sudo open golang.pkg

# setup env
export GOROOT=/usr/local/go
export GOPATH=$HOME/go
export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
```

## Install gdbclient driver

```
go get -u github.com/ailyun/alibabacloud-gdb-go-sdk/gdbclient
```
or
```
mkdir ${GOROOT}/src/github.com/aliyun
cd ${GOROOT}/src/github.com/aliyun
git clone https://github.com/ailyun/alibabacloud-gdb-go-sdk.git
cd ${GOROOT}/src/github.com/aliyun/alibabacloud-gdb-go-sdk
go install ./gdbclient
```

## Quick Examples

SDK包含有演示程序，在`examples`目录，使用以下命令运行示例

```
cd examples/add-vertex
go run main.go -host <gdb-host> -port 8182 -username root -password <password>
```

