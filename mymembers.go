package main

import (
	"encoding/json"
	"log"
	"net"
	"os"
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
