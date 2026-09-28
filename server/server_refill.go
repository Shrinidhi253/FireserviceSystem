package main

import "encoding/json"

const RefillRate = 2

func (s *FireDeptServer) TickRefill() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isBusy || !s.currentTruckArrived {
		return
	}

	s.currentTruckWater += RefillRate

	if s.currentTruckWater >= TruckWaterCap {
		s.currentTruckWater = TruckWaterCap
		s.isBusy = false
		s.sendStatus("completed")
		s.currentTruck = ""
		s.currentTruckArrived = false
		s.currentTruckConn = nil
	} else {
		s.sendStatus("in_progress")
	}
}

func (server *FireDeptServer) sendStatus(status string) {
	if server.currentTruckConn == nil {
		return
	}

	b, _ := json.Marshal(Message{Type: "refill_status", TruckID: server.currentTruck, Status: status})
	server.currentTruckConn.Write(append(b, '\n'))
}
