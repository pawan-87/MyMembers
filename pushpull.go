package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

func (m *MyMembers) handlePushPull(conn net.Conn) {
	remote, err := m.recvRemoteState(conn)
	if err != nil {
		m.logger.Printf("[ERR] handlePushPull: recv failed: %v", err)
		return
	}

	err = m.sendLocalState(conn)
	if err != nil {
		m.logger.Printf("[ERR] handlePushPull: send failed: %v", err)
		return
	}

	m.syncRemoteState(remote)
}

func (m *MyMembers) recvRemoteState(conn net.Conn) ([]pushNodeState, error) {
	header := make([]byte, 4)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header)
	if length > 10*1024*1024 {
		return nil, fmt.Errorf("push/pull state too large: %d bytes", length)
	}

	buff := make([]byte, length)
	_, err = io.ReadFull(conn, buff)
	if err != nil {
		return nil, err
	}

	var remote []pushNodeState
	err = json.Unmarshal(buff, &remote)

	return remote, err
}

func (m *MyMembers) sendLocalState(conn net.Conn) error {
	m.nodeLock.RLock()

	localState := make([]pushNodeState, 0, len(m.nodes))
	for _, ns := range m.nodes {
		localState = append(localState, pushNodeState{
			Name:        ns.Node.Name,
			Addr:        ns.Node.Addr,
			Port:        ns.Node.Port,
			Incarnation: ns.Incarnation,
			State:       ns.State,
		})
	}

	m.nodeLock.RUnlock()

	jsonBytes, err := json.Marshal(localState)
	if err != nil {
		return err
	}

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(jsonBytes)))
	_, err = conn.Write(header)
	if err != nil {
		return err
	}

	_, err = conn.Write(jsonBytes)

	return err
}

func (m *MyMembers) syncRemoteState(remote []pushNodeState) {
	for _, rs := range remote {
		switch rs.State {

		case StateAlive:
			a := alive{
				Incarnation: rs.Incarnation,
				TargetNode:  rs.Name,
				Addr:        rs.Addr,
				Port:        rs.Port,
			}
			m.aliveNode(&a)

		case StateSuspect:
			// if rs.Name node is not present in our current list
			a := alive{
				Incarnation: rs.Incarnation,
				TargetNode:  rs.Name,
				Addr:        rs.Addr,
				Port:        rs.Port,
			}
			m.aliveNode(&a)

			s := suspect{
				Incarnation: rs.Incarnation,
				Node:        rs.Name,
				From:        m.config.Name,
			}

			m.suspectNode(&s)

		case StateDead:
			// if rs.Name node is not present in our current list
			a := alive{
				Incarnation: rs.Incarnation,
				TargetNode:  rs.Name,
				Addr:        rs.Addr,
				Port:        rs.Port,
			}
			m.aliveNode(&a)

			s := dead{
				Incarnation: rs.Incarnation,
				Node:        rs.Name,
				From:        m.config.Name,
			}

			m.deadNode(&s)
		}
	}
}

func (m *MyMembers) pushPullNode(addr string) error {
	conn, err := m.transport.DialTimeout(addr, m.config.TCPTimeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(m.config.TCPTimeout))

	_, err = conn.Write([]byte{uint8(pushPullMsg)})
	if err != nil {
		return err
	}

	err = m.sendLocalState(conn)
	if err != nil {
		return err
	}

	remote, err := m.recvRemoteState(conn)
	if err != nil {
		return err
	}

	m.syncRemoteState(remote)

	return nil
}

func (m *MyMembers) pushPull() {
	m.nodeLock.RLock()
	nodes := kRandomNodes(1, m.nodes, func(ns *nodeState) bool {
		return ns.Name == m.config.Name || ns.DeadOrLeft()
	})
	m.nodeLock.RUnlock()

	if len(nodes) == 0 {
		return
	}

	addr := nodes[0].Address()
	if err := m.pushPullNode(addr); err != nil {
		m.logger.Printf("[ERR] pushPull: %v", err)
	}
}
