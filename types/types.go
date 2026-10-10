package types

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
	X           int    `json:"x,omitempty"`
	Y           int    `json:"y,omitempty"`
	LamportTime int    `json:"lamport_time,omitempty"` // Lamport logical clock value of the sender
}
