package main

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"dit-357-lab-group-6/types"

	"github.com/nats-io/nats.go"
)

// ----------------------------- Configuration ------------------------------------

const (
	GridSize     = HelpRequestExpiry
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
	BigFireCellIntensity       = MaxIntensity     //The current cell must have the highest intensity of fire
	BigFireNeighbourIntensity  = MaxIntensity - 1 //The neighbouring cells must have a fire at least 1 less than the MaxIntensity
	BigFireNeighbourFiresCount = 3                //At least 3 neighbouring cells must have a fire of BigFireNeighboutIntensity (2)
	BigFireDetectRadius        = 1                //The number of cells from the current cell that is scanned to detect a big fire

	//Call for help
	HelpMinWater    = TruckWaterCap / 2 //Only help if the water level is at least half of the maximum capacity
	HelpMaxDistance = 10                //Only help if it takes at most these many up/down/right/left steps to reach the fire
	HelpCooldown    = 10                //The no. of simulation steps a truck needs to wait before sending another help request
	HelpInboxCap    = MaxTrucks         //Maximum number of messages that the inbox can hold at a time

	// Maximum number of trucks allowed in the simulation (including initial ones).
	MaxTrucks = 6
	// Minimum number of simulation steps between two consecutive truck deployments.
	DeployCooldown = 10
	// Number of distinct concurrent help requests required to trigger a new truck deployment.
	MaxConcurrentHelpRequests = 2
	//Number of steps after which a help request no longer counts as "concurrent"
	HelpRequestExpiry = 20

	//Fuel saving parameters
	IdleThreshold  = 5 //Number of simultation steps without fire after which the truck returns to the dept
	MinFieldTrucks = 2 //Minimum number of trucks that must stay active in the field (can be changed to 1 for testing purposes)
)

// ----------------------------- Data structures ----------------------------------

type Cell struct {
	fire      bool
	intensity int
}

type TruckState string

const (
	StateExtinguishing TruckState = "EXTINGUISHING"
	StateWaiting       TruckState = "WAITING_GRANT"
	StateHeading       TruckState = "HEADING_BACK"
	StateRefill        TruckState = "REFILLING"
	StateIdle          TruckState = "SAVING_FUEL" // Idling at the fire department to save fuel
)
type Truck struct {
	id            string
	x, y          int
	water         int
	state         TruckState

	//Refill request bookkeeping.
	refillGranted bool
	refillRequested bool               // True once a request has been sent and no refill has completed yet
	grantCh         chan types.Message // Grant messages from the fire department are queued here by the NATS callback

	//Call-for-help bookkeeping
	helpInbox    chan types.Message 	//Truck inbox which stores the help messages
	isHelping    bool               	//True if this truck has agreed to help another truck
	helpX, helpY int                	//The coordinates where help is needed
	lastHelpStep int 					// The last simulation step when this truck sent a help request (used for cooldown calculation).

	//For Fuel saving	
	idleSteps int  						// Number of consecutive timesteps without encountering/fighting a fire.
	isIdle    bool 						// True if the truck returned to the fire department to save fuel.

	//For Lamport clocks	
	lamportClock int 					// Lamport logical clock for this truck.
}

type World struct {
	grid   [GridSize][GridSize]Cell
	trucks []Truck // Changed from fixed-size array to slice to support dynamic truck additions.
	step   int
}

//Returns a newly deployed truck object
func spawnNewTruck(id string, x, y int) Truck {
	return Truck {
		id: id,
		x: x, y: y,
		water: TruckWaterCap,
		state: StateExtinguishing,
		helpInbox: make(chan types.Message, HelpInboxCap), lastHelpStep: -HelpCooldown,
		grantCh: make(chan types.Message, 1),
	}
}

// countActiveInField counts trucks that are not idling at (0,0) saving fuel
func countActiveInField(w *World) int {
	active := 0
	for _, t := range w.trucks {
		if !t.isIdle {
			active++
		}
	}
	return active
}

//reports whether the truck is at the fire department
func (t *Truck) atDept() bool {
	return t.x == DeptX && t.y == DeptY
} 

//resets the fuel-saving state
func (t *Truck) endFuelSavingMode() {
	t.idleSteps = 0
	t.isIdle = false
}

//----------------------------- Lamport clocks -------------------------------------
//Lamport clock is a counter that orders events without synchronised wall clocks
//	1. Local event: clock++
//	2. Sending: clock++, then attach the value to the message
//	3. Receiving: clock = max(clock, value in message) + 1

func (t *Truck) lamportTick() {
	t.lamportClock++
}

func (t *Truck) lamportReceive(remote int) {
	t.lamportClock = max(t.lamportClock, remote) + 1
}

func (s *FireDepartmentServer) lamportTick() {
	s.lamportClock++
}

func (s *FireDepartmentServer) lamportReceive(remote int) {
	s.lamportClock = max(s.lamportClock, remote) + 1
}

//tickClocks advances every lamport clock by one local event per timestamp
func (w *World) tickClocks() {
	server.mu.Lock()
	server.lamportTick()
	server.mu.Unlock()

	for i := range w.trucks {
		w.trucks[i].lamportTick()
	}
}

// ----------------------------- Server State -------------------------------------

// refillEntry is one pending refill request waiting in the fire department's queue.
type refillEntry struct {
	TruckID string
	Lamport int    // Lamport timestamp the truck attached to its request
	Reply   string // NATS reply subject the grant must be published to
}

// refillQueue implements heap.Interface: a priority queue ordered by
// (Lamport timestamp, truck ID). The truck ID breaks ties between requests
// with equal timestamps, so the order is total and deterministic.
type refillQueue []refillEntry

func (q refillQueue) Len() int { return len(q) }
func (q refillQueue) Less(i, j int) bool {
	if q[i].Lamport != q[j].Lamport {
		return q[i].Lamport < q[j].Lamport
	}
	return q[i].TruckID < q[j].TruckID
}
func (q refillQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *refillQueue) Push(x any)   { *q = append(*q, x.(refillEntry)) }
func (q *refillQueue) Pop() any {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[:n-1]
	return x
}

// snapshot returns the queue contents in grant order (a heap's internal slice
// order is not sorted), formatted for the display and the debug trace.
func (q refillQueue) snapshot() string {
	sorted := make(refillQueue, len(q))
	copy(sorted, q)
	sort.Slice(sorted, func(i, j int) bool { return sorted.Less(i, j) })
	parts := make([]string, len(sorted))
	for i, e := range sorted {
		parts[i] = fmt.Sprintf("%s(L=%d)", e.TruckID, e.Lamport)
	}
	return strings.Join(parts, " ")
}

type FireDepartmentServer struct {
	mu           sync.Mutex
	status       string      // "FREE" or "BUSY"
	currentTruck string      // Truck ID currently holding the reservoir
	lamportClock int         // Lamport logical clock for fire department
	queue        refillQueue // Pending refill requests, ordered by (timestamp, truck_id)
}

var server = &FireDepartmentServer{
	status: "FREE",
}

// The shared NATS connection
var natsConnection *nats.Conn

// Mutex to protect concurrent access to the World's trucks slice from
// the NATS callback goroutine and the main simulation loop.
var worldMutex sync.Mutex

func startRefillServer() error {
	_, err := natsConnection.Subscribe(types.RefillSubject, handleRefillRequest)
	return err
}

// handleRefillRequest only ENQUEUES the request. It never grants directly:
// granting is done once per timestep by grantNext(), so that all requests that
// arrived during a tick are compared by (timestamp, truck_id) rather than by
// whichever message the OS/NATS happened to deliver first.
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

	// Lamport receive rule
	server.lamportReceive(request.LamportTime)

	// Ignore requests from the truck that is currently being served.
	if server.currentTruck == request.TruckID {
		return
	}
	// Ignore duplicates: the truck is already waiting in the queue.
	for _, e := range server.queue {
		if e.TruckID == request.TruckID {
			return
		}
	}

	heap.Push(&server.queue, refillEntry{
		TruckID: request.TruckID,
		Lamport: request.LamportTime,
		Reply:   message.Reply,
	})
}

// grantNext grants the reservoir to the head of the queue if the reservoir is FREE.
// The caller must hold server.mu.
func grantNext() {
	if server.status != "FREE" || server.queue.Len() == 0 {
		return
	}

	next := heap.Pop(&server.queue).(refillEntry)

	// Lamport send rule
	server.lamportTick()

	server.status = "BUSY"
	server.currentTruck = next.TruckID

	resp := types.Message{
		Type:        types.RefillGrantedMessage,
		TruckID:     next.TruckID,
		LamportTime: server.lamportClock,
	}
	respBytes, _ := json.Marshal(resp)
	natsConnection.Publish(next.Reply, respBytes)
}

// ----------------------------- Logging ------------------------------------------

var (
	logMu    sync.Mutex
	logFiles = map[string]*os.File{}
)

// logEvent appends one line to the truck's own log file (logs/<truckID>.log).
// The zero-padded Lamport value at the start of each line means that
// `cat logs/*.log | sort` gives one combined log in a globally consistent
// order (Lamport value first, truck ID as tie-break).
func logEvent(truckID string, lamport int, action, detail string) {
	logMu.Lock()
	defer logMu.Unlock()

	f, ok := logFiles[truckID]
	if !ok {
		var err error
		f, err = os.OpenFile("logs/"+truckID+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return
		}
		logFiles[truckID] = f
	}
	fmt.Fprintf(f, "L=%06d %-3s %-15s %s\n", lamport, truckID, action, detail)
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

//Decides whether to make the fire more intense or spread it or spawn new fire
func (w *World) decideFireDynamic() {
	if w.step % FireIntensifyEvery == 0 {w.intensifyFires()}
	if w.step % FireSpreadEvery == 0 	{w.spreadFires()}
	if rand.Float32() < NewFireProb		{w.addFire(rand.Intn(GridSize), rand.Intn(GridSize))}
}

// isBigFire returns true if a big fire is detected at coordinates (x, y)
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

// ----------------------------- NATS Client Logic ---------------------------------

// subscribeGrant makes the truck listen for its refill grant. The callback only
// queues the message; the truck processes it (and updates its Lamport clock) in
// act(), on the simulation goroutine. It captures the channel and not the *Truck
// because w.trucks is a slice that can be reallocated by append, which would
// leave a captured pointer pointing at a stale copy.
func subscribeGrant(t *Truck) error {
	ch := t.grantCh
	_, err := natsConnection.Subscribe(types.GrantSubject(t.id), func(m *nats.Msg) {
		var msg types.Message
		if json.Unmarshal(m.Data, &msg) != nil || msg.Type != types.RefillGrantedMessage {
			return
		}
		select {
		case ch <- msg:
		default:
		}
	})
	return err
}

// sendRefillRequest publishes ONE refill request with the truck's Lamport
// timestamp. The reply subject is the truck's own grant subject, so the fire
// department can answer later, once the request reaches the head of the queue.
func (t *Truck) sendRefillRequest() {
	// Lamport send rule
	t.lamportTick()

	request := types.Message{
		Type:        types.RefillRequestMessage,
		TruckID:     t.id,
		LamportTime: t.lamportClock,
	}
	reqBytes, _ := json.Marshal(request)
	natsConnection.PublishRequest(types.RefillSubject, types.GrantSubject(t.id), reqBytes)
	logEvent(t.id, t.lamportClock, "refill_request", "")
}

// Trucks subscribe to the HelpSubject and the callback method queues the messages
func subscribeHelp(t *Truck) error {
	_, err := natsConnection.Subscribe(types.HelpSubject, func(m *nats.Msg) { //the func() here is the callback function. It only queues the messages.
		var msg types.Message

		if json.Unmarshal(m.Data, &msg) != nil || msg.Type != types.HelpRequestMessage {
			return
		}

		t.lamportReceive(msg.LamportTime) //This is adding a timestamp
		
		select {
			case t.helpInbox <- msg: //If the message can be added to the inbox without having to wait (i.e. inbox not full), then queue the message
			default: //If the inbox is full and the message has to wait, just skip it/ ignore it.
		}
	})
	return err
}

// Method called to publish help request with lamport clock
func (t *Truck) publishHelp(x, y int) {
	//also increment clock before sending a message
	t.lamportTick()

	msg := types.Message{
		Type:        types.HelpRequestMessage,
		TruckID:     t.id,
		FireID: 	 types.FireName(x, y),	
		X:           x,
		Y:           y,
		LamportTime: t.lamportClock,
	}
	b, _ := json.Marshal(msg)
	natsConnection.Publish(types.HelpSubject, b)
}

// Trucks decide whether to help or not
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

				t.endFuelSavingMode() //If the truck is helping, it cannot be idling at the fire department to save fuel
			}
		default:
			return
		}
	}
}

// ----------------------------- Truck behaviour ----------------------------------

func (t *Truck) move(w *World, dir string) {
	// Moving is a local event: if the truck actually changed cell, tick the
	// Lamport clock and log it.
	ox, oy := t.x, t.y
	defer func() {
		if t.x != ox || t.y != oy {
			t.lamportTick()
			logEvent(t.id, t.lamportClock, "move", fmt.Sprintf("(%d,%d)->(%d,%d)", ox, oy, t.x, t.y))
		}
	}()

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
	// Extinguishing is a local event: tick the Lamport clock and log it.
	t.lamportTick()
	logEvent(t.id, t.lamportClock, "extinguish", fmt.Sprintf("(%d,%d) intensity=%d", t.x, t.y, c.intensity))
}

// Returns the closest bigFire to the truck
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

func (t *Truck) goRefill(w * World) {
	t.isHelping = false //If the water level has reached 0, the trucks cannot offer help
	t.isIdle = false    //If the truck is heading back to refill, it cannot be idling at the fire department to save fuel

	// If at (0,0) with grant, remain stationary until full
	if t.atDept() && t.refillGranted {
		t.state = StateRefill
		return
	}

	// Send exactly ONE refill request; the fire department queues it and
	// replies when this truck reaches the head of the queue.
	if !t.refillRequested {
		t.sendRefillRequest()
		t.refillRequested = true
	}

	// Check (without blocking) whether the grant has arrived.
	if !t.refillGranted {
		select {
		case g := <-t.grantCh:
			// Lamport receive rule: C = max(C, C_msg) + 1
			t.lamportReceive(g.LamportTime)

			t.refillGranted = true
			t.state = StateHeading
			logEvent(t.id, t.lamportClock, "refill_granted", "")
		default:
			t.state = StateWaiting
		}
	}

	// Already at the station: wait here (or start refilling next step)
	// instead of wandering off, since toward() returns a random direction
	// when the truck is already at its target.
	if t.atDept() {
		return
	}

	// Move towards firestation regardless of grant status
	t.move(w, toward(t.x, t.y, DeptX, DeptY))
}

func (t *Truck) callForHelpIfBigFire(w *World) {
	// Call for help when a big fire is detected
	if bx, by, ok := t.detectBigFire(w); ok && w.step-t.lastHelpStep >= HelpCooldown {
		t.publishHelp(bx, by)
		t.lastHelpStep = w.step
	}
}

func (t *Truck) driveToHelp(w *World) bool { //returns true is the truck used its turn to 
	if t.isHelping && !w.grid[t.helpX][t.helpY].fire { //If the fire is out at the location, then no need to help out
		t.isHelping = false
	}

	if t.isHelping && (t.x != t.helpX || t.y != t.helpY) { //If the truck has not reached the help location, move towards the help location
		t.endFuelSavingMode()
		t.move(w, toward(t.x, t.y, t.helpX, t.helpY))
		return true
	}
	return false
}

func (t *Truck) fightFire(w *World) bool { //it returns true if the truck used it's turn to extinguish the fire
	//Extinguising fire on current cell
	if w.grid[t.x][t.y].fire {
		t.endFuelSavingMode()
		t.extinguish(w)
		return true
	}

	//Move towards nearest fire anywhere on the grid
	fx, fy, ok := nearestFire(w, t.x, t.y)
	if ok {
		t.idleSteps = 0 //Reset idle steps when moving towards a fire
		t.isIdle = false
		t.move(w, toward(t.x, t.y, fx, fy))
		return true
	}

	return false
}

func (t *Truck) idleOrPatrol(w *World) {
	t.idleSteps++ //Increment idle steps when no fire is detected and the truck is not moving towards a fire

	//If no fire encountered for q consecutive timestamps
	if t.idleSteps >= IdleThreshold {

		// If already idling at the station, stays there until a new fire is detected
		if t.isIdle && t.atDept() {
			t.state = StateIdle
			return
		}

		// Check if returning would leave at least MinFieldTrucks active
		if countActiveInField(w) > MinFieldTrucks {
			// Check if we just reached the station (0,0)
			if t.atDept() {
				t.isIdle = true
				t.state = StateIdle // Only becomes SAVING_FUEL at (0,0)
				return
			}

			// Still on route to station
			t.isIdle = false
			t.state = StateHeading // MUST be HEADING_BACK while moving
			t.move(w, toward(t.x, t.y, DeptX, DeptY))
			return
		}
	}

	// Must stay in field (either not idle long enough, or active count would drop below minimum)
	t.isIdle = false
	t.state = StateExtinguishing
	t.move(w, randomDir())
}

func (t *Truck) act(w *World) {
	t.handleHelpRequest()

	// A truck needs water if it is at 0 or still in the process of refilling
	if t.water == 0 || t.refillGranted {
		t.goRefill(w)
		return
	}

	// Behaviour implemented when water > 0
	t.state = StateExtinguishing
	t.refillGranted = false
	t.refillRequested = false

	t.callForHelpIfBigFire(w)

	//Check if the truck can use its turn to help other truck or extinguish fire
	if t.driveToHelp(w) || t.fightFire(w) {
		return //If the turn has been used, end
	}

	//Else check if the truck needs to go into idle mode or continue patroling
	t.idleOrPatrol(w)
}

// ----------------------------- Refill Service -----------------------------------

func serviceStationRefill(w *World) {
	server.mu.Lock()
	defer server.mu.Unlock()

	// Once per timestep: if the reservoir is free, grant it to the head of the
	// (timestamp, truck_id) priority queue.
	grantNext()

	if server.status != "BUSY" {
		return
	}

	for i := range w.trucks {
		t := &w.trucks[i]
		if t.id == server.currentTruck {
			// Can only refill at the fire department (0,0) and grant permit
			if t.atDept() && t.refillGranted {
				t.water += RefillRate

				// Close connection when completely refilled
				if t.water >= TruckWaterCap {
					t.water = TruckWaterCap
					t.refillGranted = false
					t.refillRequested = false
					t.state = StateExtinguishing

					// Reservoir is now available for the next truck in the queue
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
		fmt.Printf("  %s: %d/%d [%s]", t.id, t.water, TruckWaterCap, t.state)
	}
	server.mu.Lock()
	fmt.Printf("  Station: [%s, serving:%s]  Queue: [%s]", server.status, server.currentTruck, server.queue.snapshot())
	server.mu.Unlock()
	fmt.Print("\033[K") // clear leftover characters from a longer previous line
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

	// Fresh per-truck log files for every run.
	os.RemoveAll("logs")
	if err := os.MkdirAll("logs", 0o755); err != nil {
		fmt.Printf("Could not create logs directory")
		return
	}

	if err := startRefillServer(); err != nil {
		fmt.Printf("Could not subscribe")
		return
	}

	w := World{
		trucks: []Truck{ // Slice instead of array, so we can append new trucks at runtime.
			spawnNewTruck("T1", 0, 2),
			spawnNewTruck("T2", 5, 5),
		},
	}

	//Subscribe all trucks to the help topic and to their own grant subject
	for i := range w.trucks {
		if err := subscribeHelp(&w.trucks[i]); err != nil {
			fmt.Printf("Could not subscribe to help subject")
			return
		}
		if err := subscribeGrant(&w.trucks[i]); err != nil {
			fmt.Printf("Could not subscribe to grant subject")
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

		w.tickClocks() //increment on every local event as lamport clock, one per step
		w.decideFireDynamic()

		// Remove help requests that are older than HelpRequestExpiry simulation steps.
		// This keeps the active help request count relevant and up-to-date.
		for id, step := range activeHelpRequests {
			if w.step-step > HelpRequestExpiry {
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
				
				newTruck := spawnNewTruck(newID, DeptX, DeptY)
				w.trucks = append(w.trucks, newTruck)
				
				go subscribeHelp(&w.trucks[len(w.trucks)-1]) // Subscribe the new truck to help messages.
				subscribeGrant(&w.trucks[len(w.trucks)-1])   // Subscribe the new truck to its refill grant subject.
				lastDeployStep = w.step
				activeHelpRequests = make(map[string]int) // Reset the concurrent help counter.
				worldMutex.Unlock()
			}
		default:
		}

		// Trucks act: request a refill, wait for the grant, navigate home, or extinguish.
		// Protect the trucks slice while iterating and updating truck states,
		// because new trucks may be appended by the deployment logic above.
		worldMutex.Lock()
		for i := range w.trucks {
			w.trucks[i].act(&w)
		}

		// Fire department grants the reservoir to the head of the queue (if free)
		// and refills the served truck at (0,0) until full
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

// in case our version of go doesn't have a package for max func
// this func returns the larger int among 2
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
