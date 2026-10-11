# DIT357 Lab Group 6 — Fighting Forest Fires with Distributed Messaging

A decentralised system where fire trucks coordinate over [NATS](https://nats.io) to fight fires on a 20×20 grid. The trucks share one **scarce resource**, the water reservoir at the fire department (cell `(0,0)`), and coordinate through asynchronous messaging.

**Presentation slides:** [link to slides](https://canva.link/gfnh6hns38utygl)

## Deliverables

All written deliverables are added to our wiki:

| Part | Topic | Link to wiki pages |
|------|-------|----------|
| Part 1 | Refill protocol over TCP (message catalogue, actions, arrow diagrams, transport assumptions) | [Wiki pages: Part 1](https://git.chalmers.se/shrgan/dit-357-lab-group-6/-/wikis/home#part-1) |
| Part 2 | Wireshark analysis (answers and screenshots) | [Wiki pages: Part 2](https://git.chalmers.se/shrgan/dit-357-lab-group-6/-/wikis/home#part-2) |
| Part 3 | NATS migration, naming scheme, call-for-help protocol, group discussions | [Wiki pages: Part 3](https://git.chalmers.se/shrgan/dit-357-lab-group-6/-/wikis/home#part-3) |
| Part 5 | Architecture and presentation | [Wiki pages: Part 5](https://git.chalmers.se/shrgan/dit-357-lab-group-6/-/wikis/Home#part-5) |
| Development | Development process (Kanban, reviews, design decisions) | [Wiki pages: Development](https://git.chalmers.se/shrgan/dit-357-lab-group-6/-/wikis/Home#development) |
| Project management | Agile project management (sprint plans, team contract) | [Wiki pages: Project management](https://git.chalmers.se/shrgan/dit-357-lab-group-6/-/wikis/Home#agile-project-management)

## Running the program

**Prerequisites:**<br>
1. [Go (version 1.18 or higher)](https://golang.or/dl/)
2. [Docker](https://docs.docker.com/get-docker/)


```bash
#Clone this repository
cd <to your preferred directory>

git clone https://git.chalmers.se/shrgan/dit-357-lab-group-6.git

cd dit-357-lab-group6.git

#Install the official Go client for NATS
go get github.com/nats-io/nats.go

#Start the local NATS server (make sure Docker, e.g. Docker desktop is running)
docker run -d --name nats-server -p 4222:4222 nats

#Run the go program
go run .
```


The simulation runs for 140 timesteps (600 ms each) and redraws the grid in place. 

The number of timesteps can be changed in `main.go` by setting `simTimesteps` value. The length of each timestep can be changed by changing the`TickMs` variable in the same file.

## How the system works:
1. The trucks move around the world (grid) extinguishing fires of different intensity when they see one. 
2. Extinguishing consumes the truck's water.
3. When trucks run out of water, they send a `REQUEST` to the fire department for a refill
4. The fire department sends a `GRANT` if the reservoir is free. There is only 1 fire department and the fire department can handle only one truck at a time.
5. When multiple trucks send `REQUEST`, the fire department uses `Lamport time` to determine which truck should be given the `GRANT`.
6. When trucks encounter a big fire, they call other trucks for help. The call-for-help is decentralised, i.e. the trucks decide by themselves if they want to offer help.
7. If multiple trucks call for help, the fire department deploys **additional trucks** for help.
8. The following truck actions: move, extinguish, request refill, receive refill grant are logged to `/logs/<truck id>.log`

## System architecture
![Architecture diagram](./assets/system-architecture.png)
*The architecture diagram is also added to the wiki (as a part of Part 5 deliverables)

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

<hr>

![Fighting forest fires](firetrucks_comic.png)