package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

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

)

// ----------------------------- Protocol Structs ---------------------------------

type Message struct {
	Type    string `json:"type"`     // "refill_request", "refill_granted"
	TruckID string `json:"truck_id"` // "T1", "T2"
}

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

func startTCPServer() {
	ln, err := net.Listen("tcp", "localhost:9000")
	if err != nil {
		fmt.Printf("Server listen error: %v\n", err)
		return
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleTCPRequest(conn)
	}
}

func handleTCPRequest(conn net.Conn) {
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return
	}

	var req Message
	if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &req); err != nil {
		conn.Close()
		return
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	if req.Type == "refill_request" {
		if server.status == "FREE" {
			// Grant refill and adopt this connection until refill completes
			server.status = "BUSY"
			server.currentTruck = req.TruckID
			server.activeConn = conn

			resp := Message{Type: "refill_granted", TruckID: req.TruckID}
			respBytes, _ := json.Marshal(resp)
			conn.Write(append(respBytes, '\n'))
			return // Keep connection open while refilling; closed when full
		}
		// If station is BUSY, do not send back any message.
		// It simply closes this attempt's connection. Truck will timeout and retry later.
		conn.Close()
	}
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
	clientConn    net.Conn // Saved connection to close once full
}

type World struct {
	grid   [GridSize][GridSize]Cell
	trucks [NumTrucks]Truck
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

// ----------------------------- TCP Client Logic ---------------------------------

// tryRequestRefill opens a connection and sends a refill_request
// Returns the active connection if GRANTED; returns nil if denied/timed out
func tryRequestRefill(truckID string) (net.Conn, bool) {
	conn, err := net.DialTimeout("tcp", "localhost:9000", 150*time.Millisecond)
	if err != nil {
		return nil, false
	}

	req := Message{Type: "refill_request", TruckID: truckID}
	reqBytes, _ := json.Marshal(req)
	conn.Write(append(reqBytes, '\n'))

	// Read deadline if the server does not reply because it is busy (timeout)
	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, false
	}

	var resp Message
	if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &resp); err != nil {
		conn.Close()
		return nil, false
	}

	if resp.Type == "refill_granted" {
		// Reset deadline back to normal for long-lived refill hold
		conn.SetReadDeadline(time.Time{})
		return conn, true
	}

	conn.Close()
	return nil, false
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
	// A truck needs water if it is at 0 or still in the process of refilling
	if t.water == 0 || t.refillGranted {
		// If at (0,0) with grant, remain stationary until full
		if t.x == DeptX && t.y == DeptY && t.refillGranted {
			t.state = StateRefill
			return
		}

		// If no grant yet, try to obtain one from the server
		if !t.refillGranted {
			if conn, ok := tryRequestRefill(t.id); ok {
				t.refillGranted = true
				t.clientConn = conn
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

					// Close TCP connection on client and server sides
					if t.clientConn != nil {
						t.clientConn.Close()
						t.clientConn = nil
					}
					if server.activeConn != nil {
						server.activeConn.Close()
						server.activeConn = nil
					}

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
	go startTCPServer()

	w := World{
		trucks: [NumTrucks]Truck{
			{id: "T1", x: 0, y: 2, water: TruckWaterCap, state: StateExtinguishing},
			{id: "T2", x: 5, y: 5, water: TruckWaterCap, state: StateExtinguishing},
		},
	}

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

		// Request refill over TCP, navigate home, or wait/extinguish
		for i := range w.trucks {
			w.trucks[i].act(&w)
		}

		// Fire department refills the served truck at (0,0) until full
		serviceStationRefill(&w)

		display(&w)

		if w.step >= SimTimesteps {
			fmt.Println("Simulation ended.")
			return
		}

		time.Sleep(time.Duration(TickMs) * time.Millisecond)
	}
}
