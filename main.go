package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

import "dit-357-lab-group-6/types"

// ----------------------------- Configuration ------------------------------------

const (
	GridSize     = 20
	SimTimesteps = 140
	TickMs       = 600

	// Fire dynamics
	FireSpreadEvery    = 40
	FireIntensifyEvery = 4
	NewFireProb        = 0.04
	MaxIntensity       = 3

	// Trucks and Refill parameters
	TruckWaterCap = 8
	RefillRate    = 2 // z units per timestep

	// Fire department location
	DeptX = 0
	DeptY = 0

	NumTrucks = 2

	//Big fire
	BigFireCellIntensity = MaxIntensity //The current cell must have the highest intensity of fire
	BigFireNeighbourIntensity = MaxIntensity - 1 //The neighbouring cells must have a fire at least 1 less than the MaxIntensity
	BigFireNeighbourFiresCount = 3 //At least 3 neighbouring cells must have a fire of BigFireNeighboutIntensity (2)
	BigFireDetectRadius = 1 //The number of cells from the current cell that is scanned to detect a big fire

	//Call for help
	HelpMinWater    = TruckWaterCap / 2  //Only help if the water level is at least half of the maximum capacity
	HelpMaxDistance = 10 //Only help if it takes at most 10 up/down/right/left steps to reach the fire
	HelpCooldown    = 10 //The no. of simulation steps a truck needs to wait before sending another help request
	HelpInboxCap 	= NumTrucks //Maximum number of messages that the inbox can hold at a time

	// Maximum number of trucks allowed in the simulation (including initial ones).
	MaxTrucks                 = 6
	// Minimum number of simulation steps between two consecutive truck deployments.
	DeployCooldown            = 10
	// Number of distinct concurrent help requests required to trigger a new truck deployment.
	MaxConcurrentHelpRequests = 2
)

// ----------------------------- Server State -------------------------------------

type FireDepartmentServer struct {
	mu           sync.Mutex
	status       string   // "FREE" or "BUSY"
	currentTruck string   // Truck ID currently holding the reservoir
	activeConn   net.Conn // Open connection with the truck currently refilling
}

var server = &FireDepartmentServer{
	status: "FREE",
}

//The shared NATS connection
var natsConnection *nats.Conn
// Mutex to protect concurrent access to the World's trucks slice from
// the NATS callback goroutine and the main simulation loop.
var worldMutex sync.Mutex

//Replaced startTCPServer with NATS subscription
func startRefillServer() error {
	_, err := natsConnection.Subscribe(types.RefillSubject, handleRefillRequest)
	return err
}

//Replaced handleTCPRequest 
func handleRefillRequest(message *nats.Msg) {
	var request types.Message

	if err := json.Unmarshal(message.Data, &request); err != nil {
		return
	}
	if request.Type != types.RefillRequestMessage {
		return
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	if server.status == "FREE" || server.currentTruck == request.TruckID {
		// Grant refill if the reservoir free + continue giving grant to the truck that already has the refill grant
		server.status = "BUSY"
		server.currentTruck = request.TruckID
		resp := types.Message{Type: types.RefillGrantedMessage, TruckID: request.TruckID}
		respBytes, _ := json.Marshal(resp)
		message.Respond(respBytes)
		return
	}
		// If station is BUSY, do not send back any types.Message.
}


// ----------------------------- Data structures ----------------------------------

type Cell struct {
	fire      bool
	intensity int
}

type TruckState string

const (
	StateExtinguishing TruckState = "EXTINGUISHING"
	StateWaiting  TruckState = "WAITING_GRANT"
	StateHeading  TruckState = "HEADING_BACK"
	StateRefill   TruckState = "REFILLING"
)

type Truck struct {
	id            string
	x, y          int
	water         int
	state         TruckState
	refillGranted bool

	helpInbox    chan types.Message //Truck inbox which stores the help messages
	isHelping    bool //True if this truck has agreed to help another truck
	helpX, helpY int //The coordinates where help is needed
	lastHelpStep int //The last simulation step when this truck sent a help request (used for cooldown calculation)

}

type World struct {
	grid   [GridSize][GridSize]Cell
	trucks []Truck // Changed from fixed-size array to slice to support dynamic truck additions.
	step   int
}

// ----------------------------- Fire dynamics ------------------------------------

func (w *World) addFire(x, y int) {
	if x == DeptX && y == DeptY {
		return
	}
	if !w.grid[x][y].fire {
		w.grid[x][y].fire = true
		w.grid[x][y].intensity = 1
	}
}

func (w *World) intensifyFires() {
	for i := range w.grid {
		for j := range w.grid[i] {
			c := &w.grid[i][j]
			if c.fire && c.intensity < MaxIntensity {
				c.intensity++
			}
		}
	}
}

func (w *World) spreadFires() {
	type pt struct{ x, y int }
	dirs := [4]pt{{-1, 0}, {1, 0}, {0, -1}, {0, 1}}
	var toIgnite []pt
	for i := range w.grid {
		for j := range w.grid[i] {
			if w.grid[i][j].fire {
				for _, d := range dirs {
					nx, ny := i+d.x, j+d.y
					if inBounds(nx, ny) && !w.grid[nx][ny].fire {
						toIgnite = append(toIgnite, pt{nx, ny})
					}
				}
			}
		}
	}
	for _, p := range toIgnite {
		w.addFire(p.x, p.y)
	}
}

//isBigFire returns true if a big fire is detected at coordinates (x, y)
func (w *World) isBigFire(x, y int) bool {
	if !inBounds(x, y) {
		return false
	}
	
	c := w.grid[x][y]
	if !c.fire || c.intensity < MaxIntensity {
		return false
	}

	count := 0
	for _, d := range [4][2]int{{-1, 0}, {1, 0}, {0, -1}, {0, 1}} {
		nx, ny := x+d[0], y+d[1]
		if inBounds(nx, ny) && w.grid[nx][ny].fire && w.grid[nx][ny].intensity >= BigFireNeighbourIntensity {
			count++
		}
	}
	return count >= BigFireNeighbourFiresCount
}

//Replaced the previous TCP client logic with NATS client logic
// ----------------------------- NATS Client Logic ---------------------------------

// tryRequestRefill sends a refill_request and waits for the reply.
// Returns true if GRANTED and false if denied, timed out, or nobody is listening (true/false instead of returning the TCP connection)

func tryRequestRefill(truckID string) bool {
	request := types.Message{Type: types.RefillRequestMessage, TruckID: truckID}
	reqBytes, _ := json.Marshal(request)

	// Deadline if the server does not reply because it is busy (timeout)
	reply, err := natsConnection.Request(types.RefillSubject, reqBytes, 150*time.Millisecond)
	if err != nil {
		return false
	}

	var response types.Message
	if err := json.Unmarshal(reply.Data, &response); err != nil {
		return false
	}


	return response.Type == types.RefillGrantedMessage
}


//Trucks subscribe to the HelpSubject and the callback method queues the messages
func subscribeHelp(t *Truck) error {
	_, err := natsConnection.Subscribe(types.HelpSubject, func(m *nats.Msg) { //the func() here is the callback function. It only queues the messages.
		var msg types.Message

		if json.Unmarshal(m.Data, &msg) != nil || msg.Type != types.HelpRequestMessage {
			return
		}
		select {
			case t.helpInbox <- msg: //If the message can be added to the inbox without having to wait (i.e. inbox not full), then queue the message
			default: //If the inbox is full and the message has to wait, just skip it/ ignore it.
		}
	})
	return err
}

//Method called to publish help request
func (t *Truck) publishHelp(x, y int) {
	msg := types.Message{Type: types.HelpRequestMessage, TruckID: t.id, X: x, Y: y}
	b, _ := json.Marshal(msg)
	natsConnection.Publish(types.HelpSubject, b)
}

//Trucks decide whether to help or not
func (t *Truck) handleHelpRequest() {
	for {
		select {
		case m := <-t.helpInbox:
			if m.TruckID == t.id || t.isHelping { //if the help is sent by itself or if it is already helping another truck
				continue 
			}
			dist := abs(t.x-m.X) + abs(t.y-m.Y)

			//If the truck has enough water and is not scheduled for refill and the fire is close enough, offer help
			if t.water >= HelpMinWater && !t.refillGranted && dist <= HelpMaxDistance {
				t.isHelping, t.helpX, t.helpY = true, m.X, m.Y
			}
		default:
			return
		}
	}
}

// ----------------------------- Truck behaviour ----------------------------------

func (t *Truck) move(w *World, dir string) {
	nx, ny := t.x, t.y
	switch dir {
	case "N":
		nx--
	case "S":
		nx++
	case "W":
		ny--
	case "E":
		ny++
	}

	// Direct move if cell is unoccupied and inside grid
	if inBounds(nx, ny) && !occupied(w, nx, ny, t.id) {
		t.x, t.y = nx, ny
		return
	}

	// Detour movement if obstructed, so trucks never freeze each other
	dirs := []string{"N", "S", "E", "W"}
	rand.Shuffle(len(dirs), func(i, j int) { dirs[i], dirs[j] = dirs[j], dirs[i] })

	for _, altDir := range dirs {
		ax, ay := t.x, t.y
		switch altDir {
		case "N":
			ax--
		case "S":
			ax++
		case "W":
			ay--
		case "E":
			ay++
		}
		if inBounds(ax, ay) && !occupied(w, ax, ay, t.id) {
			t.x, t.y = ax, ay
			return
		}
	}
}

func (t *Truck) extinguish(w *World) {
	c := &w.grid[t.x][t.y]
	if !c.fire || t.water == 0 {
		return
	}
	t.water--
	c.intensity--
	if c.intensity <= 0 {
		c.fire = false
		c.intensity = 0
	}
}

//Returns the closest bigFire to the truck
func (t *Truck) detectBigFire(w *World) (int, int, bool) {
	bestD := -1
	var bx, by int
	for dx := -BigFireDetectRadius; dx <= BigFireDetectRadius; dx++ {
		for dy := -BigFireDetectRadius; dy <= BigFireDetectRadius; dy++ {
			d := abs(dx) + abs(dy)
			if d > BigFireDetectRadius {
				continue // keeps the shape a diamond, not a square
			}
			x, y := t.x+dx, t.y+dy
			if w.isBigFire(x, y) && (bestD < 0 || d < bestD) {
				bestD, bx, by = d, x, y
			}
		}
	}
	return bx, by, bestD >= 0

}

func (t *Truck) act(w *World) {
	t.handleHelpRequest()

	// A truck needs water if it is at 0 or still in the process of refilling
	if t.water == 0 || t.refillGranted {
		t.isHelping = false //If the water level has reached 0, the trucks cannot offer help

		// If at (0,0) with grant, remain stationary until full
		if t.x == DeptX && t.y == DeptY && t.refillGranted {
			t.state = StateRefill
			return
		}

		// If no grant yet, try to obtain one from the server
		if !t.refillGranted {
			if tryRequestRefill(t.id) {
				t.refillGranted = true
				t.state = StateHeading
			} else {
				t.state = StateWaiting
			}
		}

		// Move towards firestation regardless of grant status
		t.move(w, toward(t.x, t.y, DeptX, DeptY))
		return
	}

	// Behaviour implemented when water > 0
	t.state = StateExtinguishing
	t.refillGranted = false

	// Call for help when a big fire is detected
	if bx, by, ok := t.detectBigFire(w); ok && w.step-t.lastHelpStep >= HelpCooldown {
		t.publishHelp(bx, by)
		t.lastHelpStep = w.step
	}

	
	if t.isHelping && !w.grid[t.helpX][t.helpY].fire { //If the fire is out at the location, then no need to help out
		t.isHelping = false
	}

	if t.isHelping && (t.x != t.helpX || t.y != t.helpY) {//If the truck has not reached the help location, move towards the help location
		t.move(w, toward(t.x, t.y, t.helpX, t.helpY))
		return
	}

	if w.grid[t.x][t.y].fire {
		t.extinguish(w)
	} else if fx, fy, ok := nearestFire(w, t.x, t.y); ok {
		t.move(w, toward(t.x, t.y, fx, fy))
	} else {
		t.move(w, randomDir())
	}
}

// ----------------------------- Refill Service -----------------------------------

func serviceStationRefill(w *World) {
	server.mu.Lock()
	defer server.mu.Unlock()

	if server.status != "BUSY" {
		return
	}

	for i := range w.trucks {
		t := &w.trucks[i]
		if t.id == server.currentTruck {
			// Can only refill at the fire department (0,0) and grant permit
			if t.x == DeptX && t.y == DeptY && t.refillGranted {
				t.water += RefillRate

				// Close connection when completely refilled
				if t.water >= TruckWaterCap {
					t.water = TruckWaterCap
					t.refillGranted = false
					t.state = StateExtinguishing

					// Reservoir is now available for the waiting truck
					server.status = "FREE"
					server.currentTruck = ""
				}
			}
			break
		}
	}
}

// ----------------------------- Helper functions ---------------------------------

func nearestFire(w *World, x, y int) (int, int, bool) {
	best := -1
	var bx, by int
	for i := range w.grid {
		for j := range w.grid[i] {
			if w.grid[i][j].fire {
				d := abs(i-x) + abs(j-y)
				if best < 0 || d < best {
					best, bx, by = d, i, j
				}
			}
		}
	}
	return bx, by, best >= 0
}

func occupied(w *World, x, y int, skipID string) bool {
	for _, t := range w.trucks {
		if t.id != skipID && t.x == x && t.y == y {
			return true
		}
	}
	return false
}

func toward(fromX, fromY, toX, toY int) string {
	dx, dy := toX-fromX, toY-fromY
	if dx == 0 && dy == 0 {
		return randomDir()
	}
	if abs(dx) > abs(dy) || (abs(dx) == abs(dy) && rand.Intn(2) == 0) {
		if dx < 0 {
			return "N"
		}
		return "S"
	}
	if dy < 0 {
		return "W"
	}
	return "E"
}

func randomDir() string {
	return []string{"N", "S", "E", "W"}[rand.Intn(4)]
}

func inBounds(x, y int) bool {
	return x >= 0 && x < GridSize && y >= 0 && y < GridSize
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// ----------------------------- Display ------------------------------------------

const displayLines = GridSize + 2

func display(w *World) {
	if w.step > 1 {
		fmt.Printf("\033[%dA", displayLines)
	}

	fmt.Print("   ")
	for j := 0; j < GridSize; j++ {
		fmt.Printf("%3d", j)
	}
	fmt.Println()

	for i := 0; i < GridSize; i++ {
		fmt.Printf("%2d ", i)
		for j := 0; j < GridSize; j++ {
			c := w.grid[i][j]
			isStation := i == DeptX && j == DeptY

			var truckOnCell *Truck
			for idx := range w.trucks {
				if w.trucks[idx].x == i && w.trucks[idx].y == j {
					truckOnCell = &w.trucks[idx]
					break
				}
			}

			// logic : always display the truck so it never disappears
			switch {
			case isStation && truckOnCell != nil:
				fmt.Print("🚒 ")
			case isStation:
				fmt.Print("🏠 ")
			case truckOnCell != nil && c.fire && truckOnCell.water > 0:
				fmt.Print("💧 ")
			case truckOnCell != nil:
				fmt.Print("🚛 ")
			case c.fire:
				switch c.intensity {
				case 1:
					fmt.Print("🟨 ")
				case 2:
					fmt.Print("🟧 ")
				case 3:
					fmt.Print("🟥 ")
				default:
					fmt.Print("🔥 ")
				}
			default:
				fmt.Print("🌲 ")
			}
		}
		fmt.Println()
	}

	fmt.Printf("Step %d", w.step)
	for _, t := range w.trucks {
		fmt.Printf("   %s: %d/%d [%s]", t.id, t.water, TruckWaterCap, t.state)
	}
	server.mu.Lock()
	fmt.Printf("   Station: [%s, serving:%s]", server.status, server.currentTruck)
	server.mu.Unlock()
	fmt.Println()
}

// ----------------------------- Main ---------------------------------------------

func main() {
	var err error
	natsConnection, err = nats.Connect(nats.DefaultURL)
	if err != nil {
		fmt.Printf("Could not connect to NATS")
		return
	}
	defer natsConnection.Close()

	if err := startRefillServer(); err != nil {
		fmt.Printf("Could not subscribe")
		return
	}

	w := World{
		trucks: []Truck{ // Slice instead of array, so we can append new trucks at runtime.
			{
				id: "T1", x: 0, y: 2, water: TruckWaterCap, state: StateExtinguishing,
				helpInbox: make(chan types.Message, HelpInboxCap), lastHelpStep: -HelpCooldown,
			},
			{
				id: "T2", x: 5, y: 5, water: TruckWaterCap, state: StateExtinguishing,
				helpInbox: make(chan types.Message, HelpInboxCap), lastHelpStep: -HelpCooldown,
			},
		},
	}

	//Subscribe all trucks to the help topic
	for i := range w.trucks {
		if err := subscribeHelp(&w.trucks[i]); err != nil {
			fmt.Printf("Could not subscribe to help subject")
			return
		}
	}

	// Channel used by the NATS callback to notify the main loop of help requests.
	deployRequestChan := make(chan string, 100)

	// Subscribe to the help subject solely for the purpose of truck deployment.
	// Every help request is forwarded to deployRequestChan.
	_, err = natsConnection.Subscribe(types.HelpSubject, func(m *nats.Msg) {
		var msg types.Message
		if json.Unmarshal(m.Data, &msg) != nil || msg.Type != types.HelpRequestMessage {
			return
		}
		select {
		case deployRequestChan <- msg.TruckID:
		default:
		}
	})
	if err != nil {
		fmt.Printf("Could not subscribe to help subject for deployment")
		return
	}

	// Tracks which trucks have requested help recently, to avoid double-counting
	// the same truck and to measure concurrent help requests.
	activeHelpRequests := make(map[string]int)
	lastDeployStep := -DeployCooldown

	w.addFire(0, 11)
	w.addFire(11, 7)
	w.addFire(15, 16)

	for {
		w.step++

		if w.step%FireIntensifyEvery == 0 {
			w.intensifyFires()
		}
		if w.step%FireSpreadEvery == 0 {
			w.spreadFires()
		}
		if rand.Float32() < NewFireProb {
			w.addFire(rand.Intn(GridSize), rand.Intn(GridSize))
		}

		// Remove help requests that are older than 20 simulation steps.
		// This keeps the active help request count relevant and up-to-date.
		for id, step := range activeHelpRequests {
			if w.step-step > 20 {
				delete(activeHelpRequests, id)
			}
		}

		// Check for new help requests and deploy a new truck if conditions are met:
		// - At least MaxConcurrentHelpRequests distinct trucks have requested help concurrently.
		// - Total number of trucks is below MaxTrucks.
		// - Enough simulation steps have passed since the last deployment (DeployCooldown).
		select {
		case truckID := <-deployRequestChan:
			activeHelpRequests[truckID] = w.step
			if len(activeHelpRequests) >= MaxConcurrentHelpRequests && len(w.trucks) < MaxTrucks && w.step-lastDeployStep >= DeployCooldown {
				worldMutex.Lock()
				newID := fmt.Sprintf("T%d", len(w.trucks)+1) // Generate T3, T4, T5, ...
				newTruck := Truck{
					id: newID, x: DeptX, y: DeptY, water: TruckWaterCap, state: StateExtinguishing,
					helpInbox: make(chan types.Message, HelpInboxCap), lastHelpStep: -HelpCooldown,
				}
				w.trucks = append(w.trucks, newTruck)
				go subscribeHelp(&w.trucks[len(w.trucks)-1]) // Subscribe the new truck to help messages.
				lastDeployStep = w.step
				activeHelpRequests = make(map[string]int) // Reset the concurrent help counter.
				worldMutex.Unlock()
			}
		default:
		}

		// Request refill over TCP, navigate home, or wait/extinguish
		// Protect the trucks slice while iterating and updating truck states,
		// because new trucks may be appended by the deployment logic above.
		worldMutex.Lock()
		for i := range w.trucks {
			w.trucks[i].act(&w)
		}

		// Fire department refills the served truck at (0,0) until full
		serviceStationRefill(&w)
		worldMutex.Unlock()

		worldMutex.Lock()
		display(&w)
		worldMutex.Unlock()
		
		if w.step >= SimTimesteps {
			fmt.Println("Simulation ended.")
			return
		}

		time.Sleep(time.Duration(TickMs) * time.Millisecond)
	}
}
