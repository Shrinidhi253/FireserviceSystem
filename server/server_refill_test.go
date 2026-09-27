package main

import "testing"

func TestTickRefill_OnlyActsWhenBusy(t *testing.T) {
	s := &FireDeptServer{isBusy: false, currentTruckArrived: true, currentTruckWater: 3}
	s.TickRefill()
	if s.currentTruckWater != 3 {
		t.Errorf("expected water unchanged while not busy, got %d", s.currentTruckWater)
	}
}

func TestTickRefill_IncreasesByZAtDepot(t *testing.T) {
	s := &FireDeptServer{isBusy: true, currentTruck: "T1", currentTruckArrived: true, currentTruckWater: 2}
	s.TickRefill()
	if s.currentTruckWater != 2+RefillRate {
		t.Errorf("expected water = %d, got %d", 2+RefillRate, s.currentTruckWater)
	}
	if !s.isBusy {
		t.Errorf("expected still busy below cap")
	}
}

func TestTickRefill_StopsAndGoesFreeAtCap(t *testing.T) {
	s := &FireDeptServer{isBusy: true, currentTruck: "T1", currentTruckArrived: true, currentTruckWater: TruckWaterCap - 1}
	s.TickRefill()
	if s.currentTruckWater != TruckWaterCap {
		t.Errorf("expected water capped at %d, got %d", TruckWaterCap, s.currentTruckWater)
	}
	if s.isBusy {
		t.Errorf("expected isBusy=false at cap")
	}
	if s.currentTruck != "" {
		t.Errorf("expected currentTruck cleared, got %q", s.currentTruck)
	}
	s.TickRefill()
	if s.currentTruckWater != TruckWaterCap {
		t.Errorf("expected water to stay at cap, got %d", s.currentTruckWater)
	}
}

func TestTickRefill_NoChangeWhenNotArrived(t *testing.T) {
	s := &FireDeptServer{isBusy: true, currentTruck: "T1", currentTruckArrived: false, currentTruckWater: 3}
	s.TickRefill()
	if s.currentTruckWater != 3 {
		t.Errorf("expected water unchanged before truck arrives, got %d", s.currentTruckWater)
	}
	if !s.isBusy {
		t.Errorf("expected isBusy unchanged before truck arrives")
	}
}
