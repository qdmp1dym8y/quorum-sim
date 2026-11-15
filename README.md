# quorum-sim

Deterministic fault-injection network for Raft election logic, written in Go.

## Why

Consensus bugs live in interleavings that CI rarely hits: a vote arrives just
after a timeout fires, a heartbeat lands across a partition, two candidates
split the vote. `quorum-sim` replaces sockets and the wall clock with a seeded
event queue, so a failing run replays message-for-message on any machine. A
seed, not a reproduction script, is the bug report.

## Fault model

The harness injects:

- crash-stop failures (`Engine.Stop`); nodes keep no state and do not restart
- latency in `[MinLatency, MaxLatency]` virtual milliseconds
- message loss with probability `DropRate`, drawn per send
- symmetric partitions (`Partition` / `Heal`)

Ties between events at the same virtual millisecond break by insertion order,
and every RNG draw comes from one seeded stream, so delivery order is total.

### Assumptions

- Nodes are deterministic and single-goroutine; no sleeps, no real I/O.
- Timers are events (`TickEvery`); clocks do not drift.
- When a link is up, latency is bounded, so heartbeats can be expected.
- Raft log replication is out of scope. Nodes carry `LastLog`, always 0.

### Safety and liveness

Safety is the harness's job to *detect*, not to grant: the `recorder` in
`cluster_test.go` fails the run if two nodes claim the same term. Liveness is
not guaranteed — `DropRate=1` or a permanent majority partition keeps the
cluster leaderless, and the engine simply stops at `Horizon`.

## Install

```sh
go get quorum-sim
```

Requires Go 1.22 or newer (for `math/rand/v2`).

## Usage

```go
ids := []sim.NodeID{"a", "b", "c"}
nodes := map[sim.NodeID]sim.Node{}
for i, id := range ids {
	peers := []sim.NodeID{}
	for _, p := range ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	nodes[id] = sim.NewRaftNode(id, peers, uint64(i)+1)
}

e, err := sim.New(sim.Config{
	Seed:       42,
	Nodes:      ids,
	MinLatency: 1,
	MaxLatency: 8,
	DropRate:   0.2,
	Horizon:    3000,
	TickEvery:  2,
}, nodes)
if err != nil {
	log.Fatal(err)
}
// Cut the oldest node off from the rest for the whole run.
e.Partition("a", "b")
e.Partition("a", "c")
if err := e.Run(); err != nil {
	log.Fatal(err)
}
for _, line := range e.Trace() {
	fmt.Println(line)
}
```

Rerun the same config and seed to get an identical trace.

## Layout

| File | Contents |
| --- | --- |
| `sim.go` | event queue, engine, tick scheduling, trace log |
| `net.go` | latency, loss and partition model |
| `cluster.go` | Raft election state machine (`RaftNode`) |
| `cluster_test.go` | determinism and single-leader-per-term checks |

## Tests

```sh
go test ./...
```

`TestTraceIsDeterministic` pins reproducibility; `TestSingleLeaderPerTerm`
and `TestPartitionedNodeFoldsBack` assert the election safety invariant under
partitions.

## License

MIT, see `LICENSE`.
