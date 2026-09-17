package main

import (
	"bufio"
	"encoding/json"
	"net"
	"sync"
)

type Message struct {
	Type    string `json:"type"`
	TruckID string `json:"truck_id"`
}

type RefillRequest struct {
	TruckID string
	Conn    net.Conn
}

type FireDeptServer struct {
	mu           sync.Mutex
	isBusy       bool
	currentTruck string
	queue        []RefillRequest
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
		s.sendGrant(req)
	}
}

func (s *FireDeptServer) sendGrant(req RefillRequest) {
	reply := Message{Type: "refill_granted", TruckID: req.TruckID}
	replyBytes, _ := json.Marshal(reply)
	req.Conn.Write(append(replyBytes, '\n'))
}