package main

import (
	"fmt"
)

func (m *MyMembers) Join(existing []string) (int, error) {
	numSuccess := 0

	for _, addr := range existing {
		err := m.pushPullNode(addr)
		if err != nil {
			m.logger.Printf("[ERR] join: push/pull with %s failed: %v", addr, err)
			continue
		}
		numSuccess++
	}

	if numSuccess == 0 {
		return 0, fmt.Errorf("failed to join any node")
	}

	return numSuccess, nil
}

func (m *MyMembers) Leave() error {
	m.nodeLock.RLock()

	self, ok := m.memberList[m.config.Name]
	if !ok {
		m.nodeLock.RUnlock()
		return fmt.Errorf("local node not found")
	}

	inc := self.Incarnation
	m.nodeLock.RUnlock()

	d := dead{
		Incarnation: inc,
		Node:        m.config.Name,
		From:        m.config.Name,
	}

	m.nodeLock.RLock()
	for _, ns := range m.nodes {
		if ns.Name == m.config.Name || ns.DeadOrLeft() {
			continue
		}
		m.encodeAndSendMsg(ns.Address(), deadMsg, &d)
	}
	m.nodeLock.RUnlock()

	m.deadNode(&d)

	return nil
}

func (m *MyMembers) Members() []*Node {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	result := make([]*Node, 0, len(m.nodes))
	for _, ns := range m.nodes {
		if ns.DeadOrLeft() {
			continue
		}
		n := ns.Node
		result = append(result, &n)
	}

	return result
}

func (m *MyMembers) LocalNode() *Node {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	ns, ok := m.memberList[m.config.Name]
	if !ok {
		return nil
	}

	n := ns.Node

	return &n
}
