package main

import (
	"fmt"
	"log"
	"time"
)

func main() {

	// node1 config
	node1Config := DefaultConfig()
	node1Config.Name = "node1"
	node1Config.BindAddr = "127.0.0.1"
	node1Config.BindPort = 6969

	node1, err := Create(node1Config)
	if err != nil {
		fmt.Printf("node1 could't initialize")
		return
	}

	// node1 config
	node2Config := DefaultConfig()
	node2Config.Name = "node2"
	node2Config.BindAddr = "127.0.0.1"
	node2Config.BindPort = 6970

	node2, err := Create(node2Config)
	if err != nil {
		fmt.Printf("node2 could't initialize")
		return
	}

	ip, port, _ := node1.transport.GetAdvertiseAddr()
	node1Addr := fmt.Sprintf("%s:%d", ip.String(), port)
	node2.Join([]string{node1Addr})

	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if len(node1.Members()) == 2 {
			fmt.Println("node2 joined the cluster!")
		}
		time.Sleep(3 * time.Second)
	}

	log.Fatalf("timeout couldn't join the cluster!")
}
