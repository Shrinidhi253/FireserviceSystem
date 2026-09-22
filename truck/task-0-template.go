package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"time"
)

/*
================================= Task 0 Template =================================
This file is a fully working centralised simulation of the firetruck scenario.
You do NOT need to write any code for Task 0.

Run it with:
    go run task-0-template.go

The simulation:
  - A 20×20 grid of forest cells. Fires ignite randomly, spread to neighbours,
    and grow in intensity over time.
  - A fire department at cell (0,0) where trucks will refill their water tanks.
  - Fire trucks roam the grid, find fires, and extinguish them using their
    on-board water supply. When a truck runs out of water it returns to the
    fire department and idles — refilling is implemented in Task 1.

Read the code and understand:
  - How the world is represented (World, Cell, Truck structs).
  - How trucks decide what to do each timestep (the act method).
  - How fire spreads and intensifies (spreadFires, intensifyFires).

From Task 1 onwards you will split this into separate processes communicating
over the network. The fire department becomes a TCP server; trucks become
clients that send explicit messages to request water refills.
====================================================================================
*/

// ----------------------------- Configuration ------------------------------------

const (
	GridSize     = 20
	SimTimesteps = 80
	TickMs       = 600

	// Fire dynamics
	FireSpreadEvery    = 40   // fires spread to neighbours every N timesteps
	FireIntensifyEvery = 4    // existing fires grow by 1 intensity every N timesteps
	NewFireProb        = 0.04 // probability of a random ignition each timestep
	MaxIntensity       = 3

	// Trucks
	TruckWaterCap = 8 // maximum water units a truck can carry

	// Fire department location
	DeptX = 0
	DeptY = 0

	NumTrucks = 2
)

// ----------------------------- Data structures ----------------------------------

type Cell struct {
	fire      bool
	intensity int
}

type Truck struct {
	id    string
	x, y  int
	water int

	connection net.Conn
	refillGranted bool
	refilling bool
	awaitingGrant bool
	waitTimesteps int
}

type World struct {
	grid   [GridSize][GridSize]Cell
	trucks [NumTrucks]Truck
	step   int
}

type Message struct {
	Type    string `json:"type"`
	TruckID string `json:"truck_id"`
	X        int    `json:"x"`
	Y        int    `json:"y"`
	Amount   int    `json:"amount"`
	Capacity int    `json:"capacity"`
	Status   string `json:"status,omitempty"`
}

// ----------------------------- Fire dynamics ------------------------------------

//Function to connect the trucks to localhost:9000
func (truck *Truck) connect() error {
	conn, err := net.Dial("tcp", "localhost:9000")

	if err != nil {return err}
	
	truck.connection = conn
	go truck.listen() //start a separate goroutine for listening to messages so that act() can continue to run while the listening
	return nil
}

//Function to listen to server messages
func (truck *Truck) listen() {
	scanner := bufio.NewScanner(truck.connection)

	for scanner.Scan() {
		var message Message

		if (json.Unmarshal(scanner.Bytes(), &message) != nil) || message.TruckID != truck.id {
			continue
		}

		switch message.Type {
			case "refill_granted":
				truck.refillGranted = true

			case "refill_status":
				switch message.Status {
					case "completed": //if the server announces that the refilling is complete, update the truck's water level accordingly
						truck.water = TruckWaterCap
						truck.refillGranted = false
						truck.refilling = false
						truck.awaitingGrant = false

					case "in_progress":
						truck.refilling = true
				}
		}
	}
}

//Function to send REQUEST messages to the server
func (truck *Truck) sendRequest() {
	b, _ := json.Marshal(Message{Type: "refill_request", TruckID: truck.id})
	truck.connection.Write(append(b, '\n'))
}

func (w *World) addFire(x, y int) {
	if x == DeptX && y == DeptY {
		return // the fire department cannot catch fire
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
	if inBounds(nx, ny) && !occupied(w, nx, ny, t.id) {
		t.x, t.y = nx, ny
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

// act decides what the truck does for one timestep.
func (t *Truck) act(w *World) {
	switch {
	case t.water == 0 || t.awaitingGrant || t.refillGranted || t.refilling:
		//out of water or needs handling the refill protocol
		t.handleRefillProtocol(w) 

	case w.grid[t.x][t.y].fire:
		// Standing on a fire: extinguish it.
		t.extinguish(w)

	default:
		// Find the nearest fire and move toward it; roam randomly if none.
		if fx, fy, ok := nearestFire(w, t.x, t.y); ok {
			t.move(w, toward(t.x, t.y, fx, fy))
		} else {
			t.move(w, randomDir())
		}
	}
}

func (truck *Truck) handleRefillProtocol(world *World) {
	
	//Case 1: GRANT has not been requested
	if (!truck.awaitingGrant) {
		truck.sendRequest()
		truck.awaitingGrant = true
		truck.waitTimesteps = 0
		return
	}

	//Case 2: When a REQUEST has been sent, and still waiting for approval
	if (truck.awaitingGrant && !truck.refillGranted) {
		truck.waitTimesteps ++

		if truck.waitTimesteps >= 5 { //If the truck has been waiting for GRANT message for more than 5 timesteps, resend the REQUEST
			truck.sendRequest()
			truck.waitTimesteps = 0
		}
		return
	}

	//Case 3: The truck has received a GRANT message and needs to move towards to fire department
	if (truck.refillGranted && !truck.refilling && !(truck.x == DeptX && truck.y == DeptY)) {
		truck.move(world, toward(truck.x, truck.y, DeptX, DeptY))
		return
	}

	//Case 4: The truck has received a GRANT message and it has arrived at the fire department
	if (truck.refillGranted && !truck.refilling && (truck.x == DeptX && truck.y == DeptY)) {
		//Notify the server that the truck has arrived at the fire department
		b, _ := json.Marshal(Message{Type: "arrived_at_dept", TruckID: truck.id})
		truck.connection.Write(append(b, '\n'))
		return
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

// displayLines is the number of terminal lines printed by display().
// Used to move the cursor back up so each frame overwrites the previous one.
const displayLines = GridSize + 2 // column header + grid rows + status line

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
			hasTruck := false
			for _, t := range w.trucks {
				if t.x == i && t.y == j {
					hasTruck = true
					break
				}
			}
			switch {
			case isStation && hasTruck:
				fmt.Print("🚒 ")
			case isStation:
				fmt.Print("🏠 ")
			case hasTruck && c.fire:
				fmt.Print("💧 ")
			case hasTruck:
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
		fmt.Printf("   %s: %d/%d water", t.id, t.water, TruckWaterCap)
	}
	fmt.Println()
}

// ----------------------------- Main ---------------------------------------------

func main() {
	
	w := World{
		trucks: [NumTrucks]Truck{
			{id: "T1", x: 0, y: 2, water: TruckWaterCap},
			{id: "T2", x: 5, y: 5, water: TruckWaterCap},
		},
	}

	// Seed a few initial fires away from the fire department.
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

		for i := range w.trucks {
			w.trucks[i].act(&w)
		}

		display(&w)

		if w.step >= SimTimesteps {
			fmt.Println("Simulation ended.")
			return
		}

		time.Sleep(time.Duration(TickMs) * time.Millisecond)
	}
}
