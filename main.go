package main

import (
    "log"
    "net"
)

// 全局存储容器（简单的键值存储）
var cache sync.Map

func main() {
    listener, err := net.Listen("tcp", ":6380")
    if err != nil {
        log.Fatal(err)
    }
    log.Println("Listening on tcp://0.0.0.0:6380")

    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Println("Accept error:", err)
            continue
        }
        log.Println("New connection from", conn.RemoteAddr())
        go startSession(conn)
    }
}
