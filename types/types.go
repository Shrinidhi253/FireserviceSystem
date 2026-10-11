package types

import "fmt"

const (
	RefillSubject = "firedept.refill"
	// GrantSubjectPrefix + truck ID is the reply subject each truck listens on
	// for its refill grant, e.g. "firedept.grant.T1".
	GrantSubjectPrefix = "firedept.grant."
	HelpSubject        = "trucks.help"

	RefillRequestMessage = "refill_request"
	RefillGrantedMessage = "refill_granted"
	HelpRequestMessage   = "help_request"
)

// Protocol structures
type Message struct {
	Type        string `json:"type"`     // "refill_request", "refill_granted", "help_request"
	TruckID     string `json:"truck_id"` // "T1", "T2"
	FireID		string `json:"fire_id,omitempty"` //structured fire name, e.g. "fire.11.7"
	X           int    `json:"x,omitempty"`
	Y           int    `json:"y,omitempty"`
	LamportTime int    `json:"lamport_time,omitempty"` // Lamport logical clock value of the sender
}

// Get the truck name in the "structured format" decided in part 3, i.e. truck.<id>
func TruckName(id string) string {
	return "truck." + id
}

// Get the fire name in the structure format, i.e. fire.<x>.<y>
func FireName(x, y int) string {
	return fmt.Sprintf("fire.%d.%d", x, y)
}

//GrantSubject is the per-truck reply subject the fire department publishes
//the refill grant to, e.g. "firedept.grant.truck.T1".
func GrantSubject(truck_id string) string {
	return GrantSubjectPrefix + TruckName(truck_id)
}