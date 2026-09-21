package main

const RefillRate = 2

func (s *FireDeptServer) TickRefill() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isBusy {
		return
	}
	if s.currentTruckX != DeptX || s.currentTruckY != DeptY {
		return
	}

	s.currentTruckWater += RefillRate

	if s.currentTruckWater >= TruckWaterCap {
		s.currentTruckWater = TruckWaterCap
		s.isBusy = false
		s.currentTruck = ""
	}
}
