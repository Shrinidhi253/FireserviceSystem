package types

const (
	RefillSubject = "firedept.refill"
	HelpSubject = "trucks.help"
	RefillRequestMessage = "refill_request"
	RefillGrantedMessage = "refill_granted"
	HelpRequestMessage = "help_request"
)

//Protocol structures
type Message struct {
	Type    string `json:"type"`     // "refill_request", "refill_granted"
	TruckID string `json:"truck_id"` // "T1", "T2"
	X       int    `json:"x,omitempty"` 
	Y       int    `json:"y,omitempty"`
}