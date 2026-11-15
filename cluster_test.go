package sim_test

import (
	"fmt"
	"reflect"
	"testing"

	sim "quorum-sim"
)

var clusterIDs = []sim.NodeID{"n1", "n2", "n3", "n4", "n5"}

// recorder observes leadership so a safety violation (two leaders in one
// term) becomes a test failure instead of a silently passing trace.
type recorder struct {
	*sim.RaftNode
	id      sim.NodeID
	elected map[uint64]sim.NodeID
	dupes   []string
}

func (r *recorder) Step(now int64, tick bool, in []sim.Msg) []sim.Out {
	outs := r.RaftNode.Step(now, tick, in)
	if r.Role() == sim.Leader {
		term := r.Term()
		if prev, ok := r.elected[term]; ok && prev != r.id {
			r.dupes = append(r.dupes, fmt.Sprintf("term %d: %s and %s", term, prev, r.id))
		}
		r.elected[term] = r.id
	}
	return outs
}

func newCluster(t *testing.T, seed uint64, drop float64, run func(*sim.Engine)) (*sim.Engine, []*recorder) {
	t.Helper()
	nodes := make(map[sim.NodeID]sim.Node, len(clusterIDs))
	elected := make(map[uint64]sim.NodeID)
	var recs []*recorder
	for i, id := range clusterIDs {
		peers := make([]sim.NodeID, 0, len(clusterIDs)-1)
		for _, p := range clusterIDs {
			if p != id {
				peers = append(peers, p)
			}
		}
		rec := &recorder{
			RaftNode: sim.NewRaftNode(id, peers, seed^uint64(i+1)*0x9e3779b97f4a7c15),
			id:       id,
			elected:  elected,
		}
		nodes[id] = rec
		recs = append(recs, rec)
	}
	e, err := sim.New(sim.Config{
		Seed:       seed,
		Nodes:      clusterIDs,
		MinLatency: 1,
		MaxLatency: 8,
		DropRate:   drop,
		Horizon:    3000,
		TickEvery:  2,
	}, nodes)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if run != nil {
		run(e)
	}
	return e, recs
}

func runCluster(t *testing.T, seed uint64, drop float64, run func(*sim.Engine)) ([]string, []*recorder) {
	t.Helper()
	e, recs := newCluster(t, seed, drop, run)
	if err := e.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return e.Trace(), recs
}

func TestTraceIsDeterministic(t *testing.T) {
	first, _ := runCluster(t, 42, 0.2, nil)
	second, _ := runCluster(t, 42, 0.2, nil)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed produced different traces")
	}

	third, _ := runCluster(t, 43, 0.2, nil)
	if reflect.DeepEqual(first, third) {
		t.Fatal("different seeds produced identical traces")
	}
}

func TestSingleLeaderPerTerm(t *testing.T) {
	_, recs := runCluster(t, 7, 0.15, func(e *sim.Engine) {
		e.Partition("n1", "n2")
		e.Partition("n1", "n3")
	})
	leaders := 0
	for _, r := range recs {
		if len(r.dupes) > 0 {
			t.Errorf("split brain: %v", r.dupes)
		}
		leaders += len(r.elected)
	}
	if leaders == 0 {
		t.Fatal("no leader elected within horizon")
	}
}

func TestPartitionedNodeFoldsBack(t *testing.T) {
	_, recs := runCluster(t, 11, 0.05, func(e *sim.Engine) {
		for _, id := range clusterIDs[1:] {
			e.Partition("n1", id)
		}
	})
	// n1 is cut from everyone; it must not win a term that another node owns.
	for _, r := range recs {
		if len(r.dupes) > 0 {
			t.Errorf("split brain: %v", r.dupes)
		}
	}
	if len(recs[0].elected) == 0 {
		t.Fatal("majority side never elected a leader")
	}
}
