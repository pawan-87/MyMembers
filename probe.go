package main

import "time"

func (m *MyMembers) probe() {
	m.nodeLock.RLock()
	numNodes := len(m.nodes)
	if numNodes <= 1 {
		m.nodeLock.RUnlock()
		return
	}

	// calculate the probe index
	if m.probeIndex >= numNodes {
		m.nodeLock.RUnlock()
		m.nodeLock.Lock()
		shuffledNodes(m.nodes)
		m.probeIndex = 0
		m.nodeLock.Unlock()
		m.nodeLock.RLock()
		numNodes = len(m.nodes)
	}

	var target *nodeState
	for i := 0; i < numNodes; i++ {
		idx := (m.probeIndex + i) % numNodes
		ns := m.nodes[idx]
		if ns.Name == m.config.Name || ns.DeadOrLeft() {
			continue
		}
		target = ns
		m.probeIndex = idx + 1
		break
	}

	m.nodeLock.RUnlock()

	if target == nil {
		return
	}

	m.probeNode(target)
}

func (m *MyMembers) probeNode(target *nodeState) {
	seqNo := m.nextSeqNo()
	ackCh := make(chan ackMessage, m.config.IndirectChecks+1)

	m.setProbeChannels(seqNo, ackCh, m.config.ProbeInterval)

	advertiseIP, advertisePort, _ := m.transport.GetAdvertiseAddr()

	p := ping{
		SeqNo:      seqNo,
		TargetNode: target.Name,
		SourceAddr: advertiseIP,
		SourcePort: uint16(advertisePort),
		SourceNode: m.config.Name,
		Broadcasts: m.getBroadcasts(0, m.config.UDPBufferSize),
	}

	err := m.encodeAndSendMsg(target.Address(), pingMsg, &p)
	if err != nil {
		m.logger.Printf("[ERR] couldn't send the ping message")
		return
	}

	probeStart := time.Now()

	select {
	case resp := <-ackCh:
		if resp.Complete {
			return
		}
	case <-time.After(m.config.ProbeTimeout):
		// Timeout - proceed to indirect ping

	case <-m.shutdownCh:
		return
	}

	// Indirect ping Starts

	// pick k random peers
	m.nodeLock.RLock()
	peers := kRandomNodes(m.config.IndirectChecks, m.nodes, func(ns *nodeState) bool {
		return ns.Name == m.config.Name || ns.Name == target.Name || ns.DeadOrLeft()
	})
	m.nodeLock.RUnlock()

	// send indirect ping requests to each peer
	for _, peer := range peers {
		req := indirectPingReq{
			SeqNo:          seqNo,
			TargetNode:     target.Name,
			TargetNodeIP:   target.Addr,
			TargetNodePort: target.Port,
			SourceAddr:     advertiseIP,
			SourcePort:     uint16(advertisePort),
			SourceNode:     m.config.Name,
		}
		err := m.encodeAndSendMsg(peer.Address(), indirectPingMsg, &req)
		if err != nil {
			m.logger.Printf("[ERR] couldn't send the ping message")
			return
		}
	}

	// wait for the remaining time
	elapsed := time.Since(probeStart)
	remaining := m.config.ProbeInterval - elapsed
	if remaining > 0 {
		select {
		case resp := <-ackCh:
			if resp.Complete {
				return
			}

		case <-time.After(remaining):

		case <-m.shutdownCh:
			return

		}
	}

	// If no ack then mark the target node suspect
	m.logger.Printf("[INFO] probe: no ack from %s, suspecting", target.Name)

	m.nodeLock.RLock()
	s := suspect{
		Incarnation: target.Incarnation,
		Node:        target.Name,
		From:        m.config.Name,
	}
	m.nodeLock.RUnlock()

	m.suspectNode(&s)
}

type ackMessage struct {
	Complete  bool
	Payload   []byte
	Timestamp time.Time
}

// =================================== Probing ===================================
func (m *MyMembers) setProbeChannels(seqNo uint32, ackCh chan ackMessage, timeout time.Duration) {

	ackFun := func(payload []byte, timestamp time.Time) {
		select {
		case ackCh <- ackMessage{Complete: true, Payload: payload, Timestamp: timestamp}:

		default:

		}
	}

	ah := &ackHandler{ackFn: ackFun, nackFn: nil, timer: nil}

	m.ackLock.Lock()
	m.ackHandlers[seqNo] = ah
	m.ackLock.Unlock()

	ah.timer = time.AfterFunc(timeout, func() {
		m.ackLock.Lock()
		delete(m.ackHandlers, seqNo)
		m.ackLock.Unlock()

		select {
		case ackCh <- ackMessage{Complete: false}:

		default:

		}
	})
}
