package main

// ServerStatus is the fire department server's status with respect to the truck it is currently servicing.
type ServerStatus int

const (
	FREE ServerStatus = iota
	BUSY
)

func (s ServerStatus) String() string {
	if s == BUSY {
		return "BUSY"
	}
	return "FREE"
}

// RefillRate is how much water is added per simTimestep while a truck is being refilled at the fire department (z = 2 in the issue).
const RefillRate = 2

// ServedTruck is the server-side view of the truck currently being serviced. Only the fields this method needs are included.
// TODO(protocol/connection issue): X, Y and Water must be kept in sync with whatever the truck reports over the network. Populate/update them wherever incoming truck-status messages are handled.
type ServedTruck struct {
	ID     string
	X, Y   int
	Water  int
	Status ServerStatus
}

// TickRefill implements issue #8. Call it once per simTimestep for the
// truck the server is currently servicing (e.g. from the server's main
// tick loop — see TODO below).
//
// TODO(server loop issue): hook this into the server's per-timestep
// loop, e.g.:
//
//	for range simTimestepTicker.C {
//	    servedTruck.TickRefill()
//	    // TODO: if servedTruck.Status flips to FREE here, send whatever
//	    // "refill complete" message the protocol defines back to the truck.
//	}
func (st *ServedTruck) TickRefill() {
	if st.Status != BUSY {
		return // criterion 1: only acts while BUSY
	}
	if st.X != DeptX || st.Y != DeptY {
		return // criterion 4: truck not at the depot, nothing changes
	}

	st.Water += RefillRate // criterion 2

	if st.Water >= TruckWaterCap {
		st.Water = TruckWaterCap
		st.Status = FREE // criterion 3
	}
}
