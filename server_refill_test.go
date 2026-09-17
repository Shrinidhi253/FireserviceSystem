package main

import "testing"

// Covers the four acceptance criteria of issue #8 in isolation.

func TestTickRefill_OnlyActsWhenBusy(t *testing.T) {
	st := &ServedTruck{X: DeptX, Y: DeptY, Water: 3, Status: FREE}
	st.TickRefill()
	if st.Water != 3 {
		t.Errorf("expected water unchanged while FREE, got %d", st.Water)
	}
}

func TestTickRefill_IncreasesByZAtDepot(t *testing.T) {
	st := &ServedTruck{X: DeptX, Y: DeptY, Water: 2, Status: BUSY}
	st.TickRefill()
	if st.Water != 2+RefillRate {
		t.Errorf("expected water = %d, got %d", 2+RefillRate, st.Water)
	}
	if st.Status != BUSY {
		t.Errorf("expected still BUSY below cap, got %s", st.Status)
	}
}

func TestTickRefill_StopsAndGoesFreeAtCap(t *testing.T) {
	st := &ServedTruck{X: DeptX, Y: DeptY, Water: TruckWaterCap - 1, Status: BUSY}
	st.TickRefill()
	if st.Water != TruckWaterCap {
		t.Errorf("expected water capped at %d, got %d", TruckWaterCap, st.Water)
	}
	if st.Status != FREE {
		t.Errorf("expected status FREE at cap, got %s", st.Status)
	}

	// Further ticks should now no-op since status is FREE.
	st.TickRefill()
	if st.Water != TruckWaterCap {
		t.Errorf("expected water to stay at cap, got %d", st.Water)
	}
}

func TestTickRefill_NoChangeWhenNotAtDepot(t *testing.T) {
	st := &ServedTruck{X: 5, Y: 5, Water: 3, Status: BUSY}
	st.TickRefill()
	if st.Water != 3 {
		t.Errorf("expected water unchanged away from depot, got %d", st.Water)
	}
	if st.Status != BUSY {
		t.Errorf("expected status unchanged away from depot, got %s", st.Status)
	}
}
