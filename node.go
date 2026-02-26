package main

import (
	"net"
	"strconv"
	"time"
)

type NodeStateType int

const (
	StateAlive NodeStateType = iota
	StateSuspect
	StateDead
)

type Node struct {
	Name  string
	Addr  net.IP
	Port  uint16
	State NodeStateType
}

func (node *Node) Address() string {
	return net.JoinHostPort(node.Addr.String(), strconv.Itoa(int(node.Port)))
}

type nodeState struct {
	Node
	Incarnation uint32
	State       NodeStateType
	StateChange time.Time
}

func (node *nodeState) DeadOrLeft() bool {
	return node.State == StateDead
}

// =================================== Messages ===================================

type messageType uint8

const (
	pingMsg messageType = iota
	indirectPingMsg
	ackRespMsg
	suspectMsg
	aliveMsg
	deadMsg
	pushPullMsg
)

type ping struct {
	SeqNo      uint32   `json:"seqNo"`                // unique ID to match this ping with its ack
	TargetNode string   `json:"target_node"`          // TargetNode node name
	SourceAddr []byte   `json:"sourceAddr"`           // Sender's IP
	SourcePort uint16   `json:"sourcePort"`           // Sender's TargetNodePort
	SourceNode string   `json:"sourceNode"`           // Sender's name
	Broadcasts [][]byte `json:"broadcasts,omitempty"` // Piggyback gossip messages
}

type ackResp struct {
	SeqNo      uint32   `json:"seqNo"`
	Payload    []byte   `json:"payload"`
	Broadcasts [][]byte `json:"broadcasts,omitempty"` // Piggyback gossip messages
}

type indirectPingReq struct {
	SeqNo uint32 `json:"seqNo"`

	// TargetNodeIP TargetNode's details
	TargetNode     string `json:"target_node"`      // name for the target node
	TargetNodeIP   []byte `json:"target_node_ip"`   // IP of the node to ping
	TargetNodePort uint16 `json:"target_node_port"` //  TargetNodePort of the target

	// Requester's details
	SourceAddr []byte `json:"sourceAddr"` // Original requester's IP
	SourcePort uint16 `json:"sourcePort"` // Original requester's port
	SourceNode string `json:"sourceNode"` // Original requester's name
}

type suspect struct {
	Incarnation uint32 `json:"incarnation"` // version of the node we're suspecting
	Node        string `json:"node"`        // who is being suspected
	From        string `json:"from"`        // who is making accusation
}

type alive struct {
	Incarnation uint32 `json:"incarnation"` // node's current incarnation number
	TargetNode  string `json:"target_node"` // who is alive
	Addr        []byte `json:"addr"`        // node's IP address
	Port        uint16 `json:"port"`
}

type dead struct {
	Incarnation uint32 `json:"incarnation"` // node's current incarnation number
	Node        string `json:"node"`        // who is dead
	From        string `json:"from"`        // who is declaring them dead
}

type ackHandler struct {
	ackFn  func([]byte, time.Time) // called when ack arrives: payload + timestamp
	nackFn func()                  // called when no ack arrives
	timer  *time.Timer
}

type pushNodeState struct {
	Name        string        `json:"name"`
	Addr        []byte        `json:"addr"`
	Port        uint16        `json:"port"`
	Incarnation uint32        `json:"incarnation"`
	State       NodeStateType `json:"state"`
}
