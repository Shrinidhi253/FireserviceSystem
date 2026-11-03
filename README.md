# DIT357 Lab Group 6 — Fighting Forest Fires with Distributed Messaging - Raw TCP implementation

This branch contains the initial version of the system which implemented truck-server communication in raw TCP (instead of a message broker).

## Message catalogue
This version only implements **truck-server** communication, i.e. refill-grant exchange. 

| Type | From -> To | JSON fields |
| ---- | ---------- | ----------- |
|`refill_request`| truck -> fire department | `type`, `truck_id`|
|`refill_grant`| fire department -> truck |`type`, `truck_id`|

## Running the program

**Prerequisites:**<br>
1. [Go (version 1.18 or higher)](https://golang.or/dl/)
2. [Wireshark](https://www.wireshark.org/download.html) (**Optional.** For tracing the TCP message segments exchanged)

```
#For tracing the messages on wireshark:
1. Open Wireshark
2. Pick the loopback interface:
        Ubuntu Linux : lo
        MacOS        : lo0
        Windows      : Adapter for loopback traffic capture

3. Apply display filter:
        tcp.port == 9000
```


```bash
#After cloning the repository (as described in main branch), and starting wireshark (if needed), run the go program

go run .
```

The simulation runs for 140 timesteps (600 ms each) and redraws the grid in place. 

The number of timesteps can be changed in `main.go` by setting `simTimesteps` value. The length of each timestep can be changed by changing the`TickMs` variable in the same file.

## Repository layout

```
|_ ...
|_ logs/             #lamport-stamped logs generated at run-time for each truck
|_ types/types.go    #shared message types and NATS subject/ naming helpers
|_ ....
|_ main.go           #main simulation, trucks, fire department, NATS logic, etc
|_ ...
```

## Team
1. Ifty Zubaer
2. Josefina Faundez Reyes
3. Lei Zhou
4. Shrinidhi Ganapathi