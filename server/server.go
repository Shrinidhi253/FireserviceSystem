package main

import (
	"bufio"
	"encoding/json"
	"net"
	"sync"
)

const (
	DeptX  = 0
	DeptY  = 0
	TickMs = 600

	TruckWaterCap = 8 // maximum water units a truck can carry
)

type Message struct {
	Type    string `json:"type"`
	TruckID string `json:"truck_id"`
	X        int    `json:"x"`
	Y        int    `json:"y"`
	Amount   int    `json:"amount"`
	Capacity int    `json:"capacity"`
	Status   string `json:"status,omitempty"`
}

type RefillRequest struct {
	TruckID string
	Conn    net.Conn
}

type FireDeptServer struct {
	mu                sync.Mutex
	isBusy            bool
	currentTruck      string
	currentTruckX     int
	currentTruckY     int
	currentTruckWater int
	currentTruckConn  net.Conn
	queue             []RefillRequest
}

func NewFireDeptServer() *FireDeptServer {
	return &FireDeptServer{
		isBusy: false,
		queue:  make([]RefillRequest, 0),
	}
}

func (s *FireDeptServer) Start() {
	listener, err := net.Listen("tcp", "localhost:9000")
	if err != nil {
		return
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *FireDeptServer) handleConnection(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var msg Message
		err := json.Unmarshal(scanner.Bytes(), &msg)
		if err != nil {
			continue
		}
		if msg.Type == "refill_request" {
			s.enqueueRequest(msg.TruckID, conn)
		}
	}
}

func (s *FireDeptServer) enqueueRequest(truckID string, conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, req := range s.queue {
		if req.TruckID == truckID {
			s.queue[i].Conn = conn
			return
		}
	}

	s.queue = append(s.queue, RefillRequest{TruckID: truckID, Conn: conn})

	if !s.isBusy && len(s.queue) > 0 {
		req := s.queue[0]
		s.queue = s.queue[1:]
		s.isBusy = true
		s.currentTruck = req.TruckID
		s.currentTruckConn = req.Conn //keep track of the connection to which we need to send refill status notifications
		s.sendGrant(req)
	}
}

func (s *FireDeptServer) sendGrant(req RefillRequest) {
	reply := Message{Type: "refill_granted", TruckID: req.TruckID}
	replyBytes, _ := json.Marshal(reply)
	req.Conn.Write(append(replyBytes, '\n'))
}
