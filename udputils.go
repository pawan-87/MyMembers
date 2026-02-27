package main

import (
	"net"
	"strconv"
	"time"
)

// packetListen it reads packets from the transport and dispatches them to correct handler
func (m *MyMembers) packetListen() {
	m.logger.Println("[DEBUG] packetListen: started packetListener")

	for {
		select {
		case pkt := <-m.transport.PacketCh():
			// handle packet
			if len(pkt.Buf) < 1 {
				continue
			}

			// extract the messageType & payload from the packet
			msgType := messageType(pkt.Buf[0])
			payload := pkt.Buf[1:]

			switch msgType {
			case pingMsg:
				m.handlePingMsg(payload, pkt.From)

			case ackRespMsg:
				m.handleAckRespMsg(payload, pkt.From, pkt.Timestamp)

			case indirectPingMsg:
				m.handleIndirectPingMsg(payload, pkt.From)

			case suspectMsg:
				m.handleSuspectMsg(payload, pkt.From)

			case aliveMsg:
				m.handleAliveMsg(payload, pkt.From)

			case deadMsg:
				m.handleDeadMsg(payload, pkt.From)

			default:
				m.logger.Printf("[WARN] unknow message type: %d", msgType)
			}

		case <-m.shutdownCh:
			return
		}
	}
}

// =================================== Message Handlers ===================================
func (m *MyMembers) handlePingMsg(payload []byte, from net.Addr) {
	var p ping
	decode(payload, &p)

	// the ping message may contains piggyback messages
	// broadcasts piggyback message to other nodes
	m.handleBroadcasts(p.Broadcasts)

	ackMsg := ackResp{
		SeqNo:      p.SeqNo,
		Broadcasts: m.getBroadcasts(0, m.config.UDPBufferSize),
	}

	var replyAddr string
	if len(p.SourceAddr) > 0 {
		ip := net.IP(p.SourceAddr)
		replyAddr = net.JoinHostPort(ip.String(), strconv.Itoa(int(p.SourcePort)))
	} else {
		replyAddr = from.String()
	}

	err := m.encodeAndSendMsg(replyAddr, ackRespMsg, ackMsg)
	if err != nil {
		m.logger.Printf("[ERR] handlePing: couldn't send the ping message")
	}
}

func (m *MyMembers) handleAckRespMsg(payload []byte, from net.Addr, timestamp time.Time) {
	var ack ackResp
	decode(payload, &ack)
	m.handleBroadcasts(ack.Broadcasts)
	m.invokeAckHandler(ack, timestamp)
}

func (m *MyMembers) handleIndirectPingMsg(payload []byte, from net.Addr) {
	var req indirectPingReq
	decode(payload, &req)

	ip := net.IP(req.TargetNodeIP)
	targetAddr := net.JoinHostPort(ip.String(), strconv.Itoa(int(req.TargetNodePort)))

	ping := &ping{
		SeqNo:      req.SeqNo,
		TargetNode: req.TargetNode,
		SourceAddr: req.SourceAddr,
		SourcePort: req.SourcePort,
		SourceNode: req.SourceNode,
	}

	err := m.encodeAndSendMsg(targetAddr, pingMsg, &ping)
	if err != nil {
		m.logger.Printf("[ERR] handleIndirectPingMsg: couldn't send the indirect ping message")
	}
}

func (m *MyMembers) handleSuspectMsg(payload []byte, from net.Addr) {
	var s suspect
	decode(payload, &s)
	m.suspectNode(&s)
}

func (m *MyMembers) handleAliveMsg(payload []byte, from net.Addr) {
	var a alive
	decode(payload, &a)
	m.aliveNode(&a)
}

func (m *MyMembers) handleDeadMsg(payload []byte, from net.Addr) {
	var d dead
	decode(payload, &d)
	m.deadNode(&d)
}

func (m *MyMembers) handleBroadcasts(broadcasts [][]byte) {

	if len(broadcasts) == 0 {
		return
	}

	for _, msg := range broadcasts {
		if len(msg) < 1 {
			continue
		}

		msgType := messageType(msg[0])
		buff := msg[1:]

		switch msgType {
		case suspectMsg:
			var s suspect
			decode(buff, &s)
			m.suspectNode(&s)

		case aliveMsg:
			var a alive
			decode(buff, &a)
			m.aliveNode(&a)

		case deadMsg:
			var d dead
			decode(buff, &d)
			m.deadNode(&d)

		default:
			m.logger.Printf("[WARN] unkown piggybacked message type: %d", msgType)
		}
	}
}
