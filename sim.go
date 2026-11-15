// Package sim provides a single-threaded, deterministic discrete-event
// network for exercising consensus protocols such as Raft.
//
// The engine owns virtual time, message order, latency and loss. Nodes are
// plain state machines: Step must be a pure function of its inputs plus a
// per-node seeded RNG, so any run replays identically from its seed.
package sim

import (
	"container/heap"
	"errors"
	"fmt"
	"math/rand/v2"
)

// NodeID identifies a simulated process.
type NodeID string

// Msg is a message that has been scheduled for delivery.
type Msg struct {
	From    NodeID
	To      NodeID
	Payload []byte
	At      int64
	Seq     uint64
}

// Out is a message a node wants to send during Step.
type Out struct {
	To      NodeID
	Payload []byte
}

// Node is a deterministic state machine. Implementations must not read the
// wall clock, iterate maps in scheduling order, or use randomness shared with
// other nodes.
type Node interface {
	// Step advances the node to virtual time now. tick reports a periodic
	// timer event; otherwise in holds the messages delivered at now.
	Step(now int64, tick bool, in []Msg) []Out
}

// Config describes a run. Latency bounds and Horizon are in virtual
// milliseconds; TickEvery is the timer period.
type Config struct {
	Seed       uint64
	Nodes      []NodeID
	MinLatency int64
	MaxLatency int64
	DropRate   float64
	Horizon    int64
	TickEvery  int64
}

type event struct {
	at   int64
	seq  uint64
	node NodeID
	tick bool
	msg  Msg
}

// eventQueue orders events by time, breaking ties by insertion order so that
// delivery is total and reproducible.
type eventQueue []event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(event)) }
func (q *eventQueue) Pop() any {
	old := *q
	ev := old[len(old)-1]
	*q = old[:len(old)-1]
	return ev
}

// Engine runs nodes against a virtual network.
type Engine struct {
	cfg   Config
	nodes map[NodeID]Node
	net   *Net
	seq   uint64
	q     eventQueue
	clock int64
	trace []string
	ran   bool
}

// New validates cfg and returns an engine whose timers fire one virtual
// millisecond apart, node by node.
func New(cfg Config, nodes map[NodeID]Node) (*Engine, error) {
	switch {
	case len(cfg.Nodes) == 0:
		return nil, errors.New("sim: no nodes")
	case cfg.Horizon <= 0:
		return nil, errors.New("sim: horizon must be positive")
	case cfg.TickEvery <= 0:
		return nil, errors.New("sim: tick period must be positive")
	case cfg.MinLatency < 0 || cfg.MaxLatency < cfg.MinLatency:
		return nil, errors.New("sim: invalid latency bounds")
	case cfg.DropRate < 0 || cfg.DropRate > 1:
		return nil, errors.New("sim: drop rate outside [0,1]")
	}
	e := &Engine{
		cfg:   cfg,
		nodes: make(map[NodeID]Node, len(cfg.Nodes)),
		net:   newNet(rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)), cfg.MinLatency, cfg.MaxLatency, cfg.DropRate),
	}
	for _, id := range cfg.Nodes {
		n, ok := nodes[id]
		if !ok {
			return nil, fmt.Errorf("sim: node %q has no implementation", id)
		}
		e.nodes[id] = n
	}
	heap.Init(&e.q)
	for i, id := range cfg.Nodes {
		e.schedule(event{at: cfg.TickEvery + int64(i), node: id, tick: true})
	}
	return e, nil
}

// Run drains the event queue or stops at Horizon. It may be called once.
func (e *Engine) Run() error {
	if e.ran {
		return errors.New("sim: engine already run")
	}
	e.ran = true
	for e.q.Len() > 0 {
		ev := heap.Pop(&e.q).(event)
		if ev.at > e.cfg.Horizon {
			break
		}
		e.clock = ev.at
		node, alive := e.nodes[ev.node]
		if !alive {
			continue
		}
		var in []Msg
		if !ev.tick {
			in = []Msg{ev.msg}
			e.trace = append(e.trace, fmt.Sprintf("%d msg %s->%s", ev.at, ev.msg.From, ev.msg.To))
		} else {
			e.trace = append(e.trace, fmt.Sprintf("%d tick %s", ev.at, ev.node))
		}
		outs := node.Step(ev.at, ev.tick, in)
		if ev.tick {
			e.schedule(event{at: ev.at + e.cfg.TickEvery, node: ev.node, tick: true})
		}
		for _, o := range outs {
			e.net.send(e, ev.at, ev.node, o)
		}
	}
	return nil
}

// Stop crashes a node: it receives nothing more and stops ticking.
func (e *Engine) Stop(id NodeID) {
	delete(e.nodes, id)
	e.trace = append(e.trace, fmt.Sprintf("%d crash %s", e.clock, id))
}

// Partition cuts the link between a and b in both directions.
func (e *Engine) Partition(a, b NodeID) { e.net.cut(a, b, true) }

// Heal restores the link between a and b.
func (e *Engine) Heal(a, b NodeID) { e.net.cut(a, b, false) }

// Time reports the virtual time of the last processed event.
func (e *Engine) Time() int64 { return e.clock }

// Trace returns the delivery log, in order, for golden comparisons.
func (e *Engine) Trace() []string {
	out := make([]string, len(e.trace))
	copy(out, e.trace)
	return out
}

func (e *Engine) schedule(ev event) {
	e.seq++
	ev.seq = e.seq
	heap.Push(&e.q, ev)
}
