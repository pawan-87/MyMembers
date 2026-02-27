package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type MyMembers struct {
	config *Config

	// Networking
	transport  *NetTransport
	broadcasts *TransmitLimitedQueue

	sequenceNum uint32 // each ping gets unique seqNo
	incarnation uint32

	// Node
	nodes      []*nodeState
	memberList map[string]*nodeState
	numNodes   uint32

	// Lock
	nodeLock sync.RWMutex // protects nodes memberList
	ackLock  sync.Mutex

	nodeTimer   map[string]*time.Timer // suspicion timers (one per suspected node)
	probeIndex  int                    // which node to probe next
	ackHandlers map[uint32]*ackHandler // Pending ack callbacks by SeqNo

	shutdown int32

	// go channels
	shutdownCh chan struct{}

	logger *log.Logger
}

func Create(conf *Config) (*MyMembers, error) {

	if conf.Logger == nil {
		conf.Logger = log.New(os.Stderr, "[mymembers] ", log.LstdFlags)
	}

	transport, err := NewNetTransport(conf.BindAddr, conf.BindPort, conf.Logger)
	if err != nil {
		conf.Logger.Printf("[Err] couldn't create tranport, %v", err)
		return nil, err
	}

	var nodes []*nodeState
	memberList := make(map[string]*nodeState)

	ip, port, err := transport.GetAdvertiseAddr()
	if err != nil {
		conf.Logger.Printf("[Err] couldn't resolve current node's address")
		return nil, err
	}

	nodeState := &nodeState{
		Node: Node{
			Name:  conf.Name,
			Addr:  ip,
			Port:  uint16(port),
			State: StateAlive,
		},
		Incarnation: 1,
		StateChange: time.Now(),
	}

	nodes = append(nodes, nodeState)
	memberList[nodeState.Name] = nodeState

	shutdownCh := make(chan struct{})

	myMembers := &MyMembers{
		config: conf,

		transport: transport,

		nodes:      nodes,
		memberList: memberList,
		numNodes:   1,

		shutdownCh: shutdownCh,

		nodeTimer:   make(map[string]*time.Timer),
		ackHandlers: make(map[uint32]*ackHandler),
	}

	myMembers.broadcasts = &TransmitLimitedQueue{
		numNodes:       func() int { return int(myMembers.estNumNodes()) },
		retransmitMult: conf.RetransmitMult,
	}

	myMembers.logger = conf.Logger

	go myMembers.packetListen()
	go myMembers.streamListen()
	go myMembers.schedule()

	return myMembers, nil
}

func (m *MyMembers) Shutdown() error {
	if atomic.LoadInt32(&m.shutdown) == 1 {
		return nil
	}

	atomic.StoreInt32(&m.shutdown, 1)
	close(m.shutdownCh)
	m.transport.Shutdown()

	return nil
}

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

// schedule runs SWIM based periodic background tasks
func (m *MyMembers) schedule() {
	m.logger.Println("[DEBUG] schedule: scheduler started")

	probTicker := time.NewTicker(m.config.ProbeInterval)
	gossipTicker := time.NewTicker(m.config.GossipInterval)
	pushPullTicker := time.NewTicker(m.config.PushPullInterval)
	defer probTicker.Stop()
	defer gossipTicker.Stop()
	defer pushPullTicker.Stop()

	for {
		select {
		case <-probTicker.C:
			m.probe()

		case <-gossipTicker.C:
			m.gossip()

		case <-pushPullTicker.C:
			m.pushPull()

		case <-m.shutdownCh:
			return
		}
	}
}

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
	s := suspect{
		Incarnation: target.Incarnation,
		Node:        target.Name,
		From:        m.config.Name,
	}
	m.suspectNode(&s)
}

func (m *MyMembers) gossip() {

	// get all pending broadcasts
	broadcasts := m.getBroadcasts(0, m.config.UDPBufferSize)
	if len(broadcasts) == 0 {
		return
	}

	m.nodeLock.RLock()
	targets := kRandomNodes(m.config.GossipNodes, m.nodes, func(ns *nodeState) bool {
		return ns.Name == m.config.Name || ns.DeadOrLeft()
	})
	m.nodeLock.RUnlock()

	// send broadcast to each target
	for _, target := range targets {
		for _, msg := range broadcasts {
			m.transport.WriteTo(msg, target.Address())
		}
	}
}

func (m *MyMembers) nextSeqNo() uint32 {
	return atomic.AddUint32(&m.sequenceNum, 1)
}

func (m *MyMembers) nextIncarnation() uint32 {
	return atomic.AddUint32(&m.incarnation, 1)
}

func (m *MyMembers) estNumNodes() uint {
	return uint(atomic.LoadUint32(&m.numNodes))
}

func encode(msgType messageType, msg interface{}) ([]byte, error) {
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		log.Printf("couldn't marshal the message: %v", msg)
		return nil, err
	}

	buff := make([]byte, 1+len(jsonBytes))
	buff[0] = uint8(msgType)
	copy(buff[1:], jsonBytes)

	return buff, nil
}

func decode(buff []byte, out interface{}) error {
	err := json.Unmarshal(buff, out)

	if err != nil {
		log.Printf("[ERR] couldn't decode message: %v", buff)
		return err
	}

	return nil
}

func (m *MyMembers) encodeAndSendMsg(addr string, msgType messageType, msg interface{}) error {
	data, err := encode(msgType, msg)
	if err != nil {
		m.logger.Printf("[ERR] mymembers: couldn't encode the message")
		return err
	}

	_, err = m.transport.WriteTo(data, addr)
	if err != nil {
		m.logger.Printf("[ERR] mymembers: couldn't send the message")
		return err
	}

	return nil
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

func (m *MyMembers) invokeAckHandler(ack ackResp, timestamp time.Time) {
	m.ackLock.Lock()
	ah, ok := m.ackHandlers[ack.SeqNo]
	delete(m.ackHandlers, ack.SeqNo)
	m.ackLock.Unlock()

	if ok {
		ah.timer.Stop()
		ah.ackFn(ack.Payload, timestamp)
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

// getBroadcasts
func (m *MyMembers) getBroadcasts(overhead int, limit int) [][]byte {
	if m.broadcasts == nil {
		return nil
	}
	return m.broadcasts.GetBroadcasts(overhead, limit)
}

// =================================== Update TargetNode States ===================================

func (m *MyMembers) aliveNode(a *alive) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()

	existing, ok := m.memberList[a.TargetNode]
	if !ok {
		// if unknown then add as new node
		ns := &nodeState{
			Node: Node{
				Name:  a.TargetNode,
				Addr:  net.IP(a.Addr),
				Port:  a.Port,
				State: StateAlive,
			},
			Incarnation: a.Incarnation,
			State:       StateAlive,
			StateChange: time.Now(),
		}

		m.nodes = append(m.nodes, ns)
		m.memberList[a.TargetNode] = ns
		atomic.AddUint32(&m.numNodes, 1)

		// Queue broadcast so others learn about this node
		encoded, _ := encode(aliveMsg, a)
		m.broadcasts.QueueBroadcast(a.TargetNode, encoded)

		m.logger.Printf("[INFO] new node: %s at %s:%d", a.TargetNode, net.IP(a.Addr), a.Port)
		return
	}

	// if the current node
	if a.TargetNode == m.config.Name {
		if a.Incarnation <= atomic.LoadUint32(&m.incarnation) {
			return
		}
		atomic.StoreUint32(&m.incarnation, a.Incarnation)
		return
	}

	if a.Incarnation <= existing.Incarnation && existing.State == StateAlive {
		// already know about this or newer
		return
	}
	if a.Incarnation < existing.Incarnation {
		// older packets
		return
	}

	existing.Incarnation = a.Incarnation
	existing.State = StateAlive
	existing.Node.State = StateAlive
	existing.Addr = a.Addr
	existing.Port = a.Port
	existing.StateChange = time.Now()

	if timer, ok := m.nodeTimer[a.TargetNode]; ok {
		timer.Stop()
		delete(m.nodeTimer, a.TargetNode)
	}

	// Queue broadcast so others learn about this node
	encoded, err := encode(aliveMsg, a)
	if err == nil {
		m.broadcasts.QueueBroadcast(a.TargetNode, encoded)
	}
}

func (m *MyMembers) suspectNode(s *suspect) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()

	existing, ok := m.memberList[s.Node]
	if !ok {
		m.logger.Printf("[ERR] suspectNode: couldn't find node")
		return
	}

	if s.Node == m.config.Name {
		m.refute(existing, s.Incarnation)
		return
	}

	if s.Incarnation < existing.Incarnation {
		return
	}

	// already suspect or dead at the same incarnation - ignore
	if existing.State != StateAlive {
		return
	}

	existing.Incarnation = s.Incarnation
	existing.State = StateSuspect
	existing.Node.State = StateSuspect
	existing.StateChange = time.Now()

	encoded, _ := encode(suspectMsg, s)
	m.broadcasts.QueueBroadcast(s.Node, encoded)

	// Start the suspicion timer

	timeout := suspicionTimeout(m.config.SuspicionMult, int(m.estNumNodes()), m.config.ProbeInterval)

	timer := time.AfterFunc(timeout, func() {
		d := dead{
			Incarnation: s.Incarnation,
			Node:        s.Node,
			From:        m.config.Name,
		}
		m.deadNode(&d)
	})

	if exiting, ok := m.nodeTimer[s.Node]; ok {
		exiting.Stop()
	}

	m.nodeTimer[s.Node] = timer
}

func (m *MyMembers) deadNode(d *dead) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()

	exiting, ok := m.memberList[d.Node]
	if !ok {
		m.logger.Printf("[ERR] deadNode: couldn't find node")
		return
	}

	if d.Incarnation < exiting.Incarnation {
		return
	}

	if exiting.State == StateDead {
		return
	}

	if d.Node == m.config.Name {
		if d.From == d.Node {
			// forcefully leaving the cluster
		} else {
			m.refute(exiting, d.Incarnation)
			return
		}
	}

	exiting.Incarnation = d.Incarnation
	exiting.State = StateDead
	exiting.Node.State = StateDead
	exiting.StateChange = time.Now()

	if timer, ok := m.nodeTimer[d.Node]; ok {
		timer.Stop()
		delete(m.nodeTimer, d.Node)
	}

	encoded, _ := encode(deadMsg, d)
	m.broadcasts.QueueBroadcast(d.Node, encoded)

	atomic.StoreUint32(&m.numNodes, uint32(len(m.nodes)))
}

// refute called when someone suspects or declares us dead
func (m *MyMembers) refute(me *nodeState, accusedInc uint32) {
	newInc := atomic.AddUint32(&m.incarnation, 1)

	for newInc <= accusedInc {
		newInc = atomic.AddUint32(&m.incarnation, 1)
	}

	me.Incarnation = newInc
	me.State = StateAlive
	me.Node.State = StateAlive
	me.StateChange = time.Now()

	a := alive{
		Incarnation: newInc,
		TargetNode:  m.config.Name,
		Addr:        me.Addr,
		Port:        me.Port,
	}

	encoded, _ := encode(aliveMsg, &a)
	m.broadcasts.QueueBroadcast(m.config.Name, encoded)
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
