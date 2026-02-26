package main

import (
	"log"
	"os"
	"time"
)

type Config struct {
	Name string // unique name of the node

	// Network config
	BindAddr   string // IP address to listen on
	BindPort   int    // UDP port to listen on
	TCPTimeout time.Duration

	// Failure detection config
	ProbeInterval  time.Duration // protocol period
	ProbeTimeout   time.Duration
	IndirectChecks int // how many random nodes to ping on your behalf

	PushPullInterval time.Duration // how often to do periodic push/pull with random node

	// Suspicion
	SuspicionMult int // (in sec) how long a node stays in "suspect" before being declared dead

	// Gossip Dissemination
	RetransmitMult int           // (in sec) how many times a state changes is broadcast
	GossipInterval time.Duration // how often pending state changes are pushed to random nodes
	GossipNodes    int           // how many random nodes receive gossip each round

	// Transport
	UDPBufferSize int // maximum size of a single UDP packet in bytes

	// Logging
	Logger *log.Logger
}

func DefaultConfig() *Config {
	name, _ := os.Hostname()

	config := &Config{
		Name: name,

		BindAddr:   "0.0.0.0",
		BindPort:   6969,
		TCPTimeout: 10 * time.Second,

		ProbeInterval:  1 * time.Second,
		ProbeTimeout:   500 * time.Millisecond,
		IndirectChecks: 3,

		PushPullInterval: 30 * time.Second,

		SuspicionMult: 4,

		RetransmitMult: 4,
		GossipInterval: 200 * time.Millisecond,
		GossipNodes:    3,

		UDPBufferSize: 1400,
		Logger:        nil,
	}

	return config
}
