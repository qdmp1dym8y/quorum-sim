package chaos_test

import (
	"strings"
	"testing"

	sim "quorum-sim"
	"quorum-sim/chaos"
)

type noop struct{}

func (noop) Step(int64, bool, []sim.Msg) []sim.Out { return nil }

func TestNewValidates(t *testing.T) {
	cases := []struct {
		name   string
		faults []chaos.Fault
	}{
		{"negative time", []chaos.Fault{{At: -1, Kind: chaos.Crash, A: "a"}}},
		{"same endpoints", []chaos.Fault{{At: 1, Kind: chaos.Partition, A: "a", B: "a"}}},
		{"missing endpoint", []chaos.Fault{{At: 1, Kind: chaos.Heal, A: "a"}}},
		{"crash without node", []chaos.Fault{{At: 1, Kind: chaos.Crash}}},
		{"unknown kind", []chaos.Fault{{At: 1, Kind: chaos.Kind(9), A: "a"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := chaos.New(tc.faults); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestFaultsFireInVirtualTimeOrder(t *testing.T) {
	s, err := chaos.New([]chaos.Fault{
		{At: 30, Kind: chaos.Crash, A: "b"},
		{At: 10, Kind: chaos.Partition, A: "a", B: "b"},
		{At: 20, Kind: chaos.Heal, A: "a", B: "b"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := sim.New(sim.Config{
		Seed:      1,
		Nodes:     []sim.NodeID{"a", "b"},
		TickEvery: 5,
		Horizon:   60,
		OnEvent:   s.Hook(),
	}, map[sim.NodeID]sim.Node{"a": noop{}, "b": noop{}})
	if err != nil {
		t.Fatalf("sim.New: %v", err)
	}
	if err := e.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if s.Remaining() != 0 {
		t.Fatalf("%d faults never fired", s.Remaining())
	}

	trace := e.Trace()
	crashAt := -1
	for i, line := range trace {
		if line == "30 crash b" {
			crashAt = i
			break
		}
	}
	if crashAt < 0 {
		t.Fatalf("crash not recorded in trace: %v", trace)
	}
	for _, line := range trace[crashAt+1:] {
		if strings.Contains(line, "tick") && strings.HasSuffix(line, " b") {
			t.Errorf("crashed node still ticked: %q", line)
		}
	}
}

// recorder fails the run if two nodes lead the same term.
type recorder struct {
	*sim.RaftNode
	id    sim.NodeID
	terms map[uint64]sim.NodeID
	dupes []string
}

func (r *recorder) Step(now int64, tick bool, in []sim.Msg) []sim.Out {
	outs := r.RaftNode.Step(now, tick, in)
	if r.Role() == sim.Leader {
		term := r.Term()
		if prev, ok := r.terms[term]; ok && prev != r.id {
			r.dupes = append(r.dupes, string(prev)+" and "+string(r.id))
		}
		r.terms[term] = r.id
	}
	return outs
}

// runCrash crashes n1 and n2 at t=300 and returns the term map plus any
// split-brain findings.
func runCrash(t *testing.T, seed uint64) (map[uint64]sim.NodeID, []string) {
	t.Helper()
	ids := []sim.NodeID{"n1", "n2", "n3", "n4", "n5"}
	s, err := chaos.New([]chaos.Fault{
		{At: 300, Kind: chaos.Crash, A: "n1"},
		{At: 301, Kind: chaos.Crash, A: "n2"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	nodes := make(map[sim.NodeID]sim.Node, len(ids))
	terms := make(map[uint64]sim.NodeID)
	recs := make([]*recorder, 0, len(ids))
	for i, id := range ids {
		peers := make([]sim.NodeID, 0, len(ids)-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		rec := &recorder{RaftNode: sim.NewRaftNode(id, peers, seed+uint64(i)), id: id, terms: terms}
		nodes[id] = rec
		recs = append(recs, rec)
	}
	e, err := sim.New(sim.Config{
		Seed:       seed,
		Nodes:      ids,
		MinLatency: 1,
		MaxLatency: 6,
		Horizon:    3000,
		TickEvery:  2,
		OnEvent:    s.Hook(),
	}, nodes)
	if err != nil {
		t.Fatalf("sim.New: %v", err)
	}
	if err := e.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var dupes []string
	for _, r := range recs {
		dupes = append(dupes, r.dupes...)
	}
	return terms, dupes
}

func TestCrashForcesReelectionWithoutSplitBrain(t *testing.T) {
	reelected := false
	for seed := uint64(1); seed <= 20 && !reelected; seed++ {
		terms, dupes := runCrash(t, seed)
		if len(dupes) > 0 {
			t.Fatalf("seed %d split brain: %v", seed, dupes)
		}
		if len(terms) >= 2 {
			reelected = true
		}
	}
	if !reelected {
		t.Fatal("no seed produced a re-election after crashing two nodes")
	}
}
