package main

import (
	"sort"
	"sync"
)

// thread-safe queue that manages gossip broadcasts
// each message is retransmitted a limited number of times
// then automatically removed

type limitedBroadcast struct {
	node      string // which node this broadcast is about
	msg       []byte
	transmits int // how many times this has been sent so far
}

type TransmitLimitedQueue struct {
	mu             sync.Mutex
	queue          []*limitedBroadcast // the pending broadcasts
	numNodes       func() int          // callback to get current cluster size
	retransmitMult int
}

func (q *TransmitLimitedQueue) QueueBroadcast(node string, msg []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()

	newQueue := make([]*limitedBroadcast, 0, len(q.queue))
	for _, b := range q.queue {
		if b.node != node {
			newQueue = append(newQueue, b)
		}
	}

	newQueue = append(newQueue, &limitedBroadcast{
		node:      node,
		msg:       msg,
		transmits: 0,
	})

	q.queue = newQueue
}

func (q *TransmitLimitedQueue) GetBroadcasts(overhead, limit int) [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.queue == nil || len(q.queue) < 1 {
		return nil
	}

	maxTransmits := retransmitLimit(q.retransmitMult, q.numNodes())

	sort.Slice(q.queue, func(i, j int) bool {
		return q.queue[i].transmits < q.queue[j].transmits
	})

	bytesAvailable := limit - overhead
	var result [][]byte

	for i, b := range q.queue {
		if len(b.msg) > bytesAvailable {
			continue
		}
		bytesAvailable -= len(b.msg)
		result = append(result, b.msg)
		q.queue[i].transmits++
	}

	// remove broadcast messages
	remaining := make([]*limitedBroadcast, 0, len(q.queue))
	for _, b := range q.queue {
		if b.transmits < maxTransmits {
			remaining = append(remaining, b)
		}
	}

	q.queue = remaining

	return result
}

func (q *TransmitLimitedQueue) NumQueued() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	n := len(q.queue)

	return n
}
