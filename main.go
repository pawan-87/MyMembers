package main

import (
	"fmt"
)

func main() {

	// node1 config
	node1Config := &Config{
		Name:     "node1",
		BindAddr: "0.0.0.0",
		BindPort: 6969,
	}

	node1, err := Create(node1Config)
	if err != nil {
		fmt.Printf("Couln't initialize the nod1")
		return
	}

	ip, port, _ := node1.transport.GetAdvertiseAddr()
	fmt.Printf("Node1 initialized! nodeAddress: %v:%d\n", ip.String(), port)

	//// node2 config
	//node2Config := &Config{
	//	Name:     "node2",
	//	BindAddr: "0.0.0.0",
	//	BindPort: 6970,
	//}
}
