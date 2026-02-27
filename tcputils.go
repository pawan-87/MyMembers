package main

import (
	"io"
	"net"
	"time"
)

func (m *MyMembers) streamListen() {
	m.logger.Println("[DEBUG] streamListen: started streamListener")

	for {
		select {
		case conn := <-m.transport.StreamCh():
			go m.handleStream(conn)
		case <-m.shutdownCh:
			return
		}
	}
}

func (m *MyMembers) handleStream(conn net.Conn) {
	conn.SetDeadline(time.Now().Add(m.config.TCPTimeout))
	defer conn.Close()

	buff := make([]byte, 1)
	_, err := io.ReadFull(conn, buff)
	if err != nil {
		m.logger.Printf("[ERR] stream: failed to read message type: %v", err)
		return
	}

	switch messageType(buff[0]) {
	case pushPullMsg:
		m.handlePushPull(conn)
	default:
		m.logger.Printf("[ERR] stream: unknown message type: %d", buff[0])
	}
}
