package main

import (
	"math"
	"math/rand"
	"time"
)

// kRandomNodes  pick k random nodes from a list
func kRandomNodes(k int, nodes []*nodeState, exclude func(state *nodeState) bool) []*nodeState {

	n := len(nodes)
	copied := make([]*nodeState, n)
	copy(copied, nodes)

	shuffledNodes(copied)

	result := make([]*nodeState, 0, k)

	for _, ns := range copied {
		if exclude != nil && exclude(ns) {
			continue
		}
		result = append(result, ns)
		if len(result) >= k {
			break
		}
	}

	return result
}

func shuffledNodes(nodes []*nodeState) {
	n := len(nodes)
	for i := n - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
}

// suspicionTimeout calculate how long to suspect before declaring dead
func suspicionTimeout(suspicionMult int, n int, interval time.Duration) time.Duration {
	nodeCount := n
	if nodeCount < 1 {
		nodeCount = 1
	}
	logN := math.Log10(float64(nodeCount))
	timeout := time.Duration(float64(suspicionMult) * logN * float64(interval))
	return timeout
}

func retransmitLimit(retransmitMult int, n int) int {
	nodeCount := n + 1
	if nodeCount < 2 {
		nodeCount = 2
	}
	limit := int(math.Ceil(float64(retransmitMult) * math.Log10(float64(nodeCount))))
	return limit
}
