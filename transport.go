package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Packet struct {
	Buf       []byte
	From      net.Addr  // Sender's address
	Timestamp time.Time // when the packet was received (requires for measuring round-trip time (RTT) for pings)
}

type NetTransport struct {
	conn      *net.UDPConn // single UDP socket for both send and receive
	tcpLister net.Listener // tcp socket

	packetCh chan *Packet // channel that listener goroutine pushes received packets into

	streamCh chan net.Conn

	shutdown int32 // set to one shen shutting down

	wg sync.WaitGroup // tracks the listener goroutine for clean shutdown

	logger *log.Logger
}

func NewNetTransport(bindAddr string, bindPort int, logger *log.Logger) (*NetTransport, error) {

	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", bindAddr, bindPort))
	if err != nil {
		logger.Printf("[ERR] transport: couldn't resolve the udp address: %v", err)
		return nil, err
	}

	// create udp socket
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		logger.Printf("[ERR] transport: couldn't create udp connection: %v", err)
		return nil, err
	}

	// if the bind port was sent 0 the OS assigned a random port for UDP
	// since we want the tcp on the same port we have to get the actual port assigned to UDP
	actualPort := udpConn.LocalAddr().(*net.UDPAddr).Port
	tcpAddr := fmt.Sprintf("%s:%d", bindAddr, actualPort)
	tcpLn, err := net.Listen("tcp", tcpAddr)
	if err != nil {
		logger.Printf("[ERR] transport: couldn't start the tcp connection")
		return nil, err
	}

	netTransport := &NetTransport{
		conn:      udpConn,
		tcpLister: tcpLn,
		shutdown:  0,
		packetCh:  make(chan *Packet, 1024),
		streamCh:  make(chan net.Conn),
		logger:    logger,
	}

	netTransport.wg.Add(1)
	netTransport.wg.Add(1)
	go netTransport.udpListen()
	go netTransport.tcpListen()

	return netTransport, nil
}

func (t *NetTransport) Shutdown() error {
	atomic.StoreInt32(&t.shutdown, 1)

	err := t.conn.Close()
	if err != nil {
		return err
	}

	err = t.tcpLister.Close()
	if err != nil {
		return err
	}

	t.wg.Wait()
	return nil
}

// GetAdvertiseAddr Get the current node's IP address
func (t *NetTransport) GetAdvertiseAddr() (net.IP, int, error) {
	addr := t.conn.LocalAddr().(*net.UDPAddr)
	ip := addr.IP
	return ip, addr.Port, nil
}

// =================================== UDP ===================================
func (t *NetTransport) udpListen() {
	defer t.wg.Done()

	for {
		buf := make([]byte, 65536) // 65536 max UDP datagram size
		n, addr, err := t.conn.ReadFromUDP(buf)
		ts := time.Now()
		t.logger.Printf("[DBG] transport: UDP Packet received!")

		if err != nil {
			if atomic.LoadInt32(&t.shutdown) == 1 {
				break
			}
			t.logger.Printf("[ERR] transport: UDP read error: %v", err)
			continue
		}

		if n < 1 {
			t.logger.Printf("no data")
			continue
		}

		t.packetCh <- &Packet{
			Buf:       buf[:n],
			From:      addr,
			Timestamp: ts,
		}
	}
}

func (t *NetTransport) WriteTo(data []byte, addr string) (time.Time, error) {

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		fmt.Printf("[ERR] transport: couldn't resolve the udp address: %v", err)
		return time.Now(), err
	}

	_, err = t.conn.WriteTo(data, udpAddr)
	if err != nil {
		fmt.Printf("[ERR] transport: couldn't send data: %v", err)
		return time.Now(), err
	}

	return time.Now(), err
}

func (t *NetTransport) PacketCh() <-chan *Packet {
	return t.packetCh
}

// =================================== TCP ===================================
func (t *NetTransport) tcpListen() {
	defer t.wg.Done()

	for {
		conn, err := t.tcpLister.Accept()
		if err != nil {
			if atomic.LoadInt32(&t.shutdown) == 1 {
				break
			}
			t.logger.Printf("[ERR] transport: TCP accept error: %v", err)
			continue
		}

		t.streamCh <- conn
	}
}

func (t *NetTransport) StreamCh() <-chan net.Conn {
	return t.streamCh
}

// DialTimeout [outgoing TCP connections] send push message to other nodes
func (t *NetTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}
