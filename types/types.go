package types

const (
	RefillSubject = "firedept.refill"
	RefillRequestMessage = "refill_request"
	RefillGrantedMessage = "refill_granted"
)

//Protocol structures
type Message struct {
	Type    string `json:"type"`     // "refill_request", "refill_granted"
	TruckID string `json:"truck_id"` // "T1", "T2"
}