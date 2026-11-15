package sim

import (
	"encoding/json"
	"math/rand/v2"
)

// Role is the Raft state of a node.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// Election timers are measured in ticks, configured so a partitioned node
// retries often enough to fold back after a heal.
const (
	electionMin    = 12
	electionMax    = 24
	heartbeatEvery = 4
)

// Wire is the JSON payload exchanged between RaftNode values.
type Wire struct {
	Term    uint64 `json:"term"`
	Kind    string `json:"kind"`
	Granted bool   `json:"granted,omitempty"`
	LastLog uint64 `json:"last_log"`
}

// RaftNode implements the election half of Raft: terms, one vote per term,
// heartbeats and randomised timeouts. Log replication is out of scope, so
// LastLog always stays 0 and is only compared for shape.
type RaftNode struct {
	id       NodeID
	peers    []NodeID
	role     Role
	term     uint64
	votedFor NodeID
	leader   NodeID
	lastLog  uint64
	deadline int64
	lastBeat int64
	votes    int
	rng      *rand.Rand
}

// NewRaftNode builds a follower. Each node needs its own seed so timeouts do
// not stay in lockstep.
func NewRaftNode(id NodeID, peers []NodeID, seed uint64) *RaftNode {
	peers = append([]NodeID(nil), peers...)
	return &RaftNode{
		id:    id,
		peers: peers,
		rng:   rand.New(rand.NewPCG(seed, seed|1)),
	}
}

// Role reports the current role.
func (r *RaftNode) Role() Role { return r.role }

// Term reports the current term.
func (r *RaftNode) Term() uint64 { return r.term }

// Leader reports the last known leader, empty if unknown.
func (r *RaftNode) Leader() NodeID { return r.leader }

// Step applies one timer tick or a batch of messages.
func (r *RaftNode) Step(now int64, tick bool, in []Msg) []Out {
	if r.deadline == 0 {
		r.deadline = r.nextDeadline(now)
	}
	var outs []Out
	for _, m := range in {
		w, ok := decode(m.Payload)
		if !ok {
			continue
		}
		if w.Term > r.term {
			r.term = w.Term
			r.role = Follower
			r.votedFor = ""
			r.leader = ""
			r.deadline = r.nextDeadline(now)
		}
		switch w.Kind {
		case "vote":
			granted := w.Term == r.term &&
				(r.votedFor == "" || r.votedFor == m.From) &&
				w.LastLog >= r.lastLog
			if granted {
				r.votedFor = m.From
				r.deadline = r.nextDeadline(now)
			}
			outs = append(outs, r.out(m.From, Wire{Term: r.term, Kind: "vote_reply", Granted: granted}))
		case "vote_reply":
			if r.role == Candidate && w.Term == r.term && w.Granted {
				r.votes++
				if r.votes*2 > len(r.peers)+1 {
					r.becomeLeader(now)
				}
			}
		case "append":
			if w.Term == r.term {
				r.role = Follower
				r.leader = m.From
				r.deadline = r.nextDeadline(now)
			}
			outs = append(outs, r.out(m.From, Wire{Term: r.term, Kind: "append_reply"}))
		}
	}
	if !tick {
		return outs
	}
	if r.role == Leader {
		if now-r.lastBeat >= heartbeatEvery {
			r.lastBeat = now
			for _, p := range r.peers {
				outs = append(outs, r.out(p, Wire{Term: r.term, Kind: "append"}))
			}
		}
		return outs
	}
	if now >= r.deadline {
		outs = append(outs, r.campaign(now)...)
	}
	return outs
}

func (r *RaftNode) campaign(now int64) []Out {
	r.role = Candidate
	r.term++
	r.votedFor = r.id
	r.leader = ""
	r.votes = 1
	r.deadline = r.nextDeadline(now)
	if r.votes*2 > len(r.peers)+1 {
		r.becomeLeader(now)
		return nil
	}
	outs := make([]Out, 0, len(r.peers))
	for _, p := range r.peers {
		outs = append(outs, r.out(p, Wire{Term: r.term, Kind: "vote", LastLog: r.lastLog}))
	}
	return outs
}

func (r *RaftNode) becomeLeader(now int64) {
	r.role = Leader
	r.leader = r.id
	r.lastBeat = now
	r.deadline = now + electionMax
}

func (r *RaftNode) nextDeadline(now int64) int64 {
	return now + electionMin + r.rng.Int64N(electionMax-electionMin+1)
}

func (r *RaftNode) out(to NodeID, w Wire) Out {
	payload, err := json.Marshal(w)
	if err != nil {
		panic(err) // Wire is a fixed struct; Marshal cannot fail.
	}
	return Out{To: to, Payload: payload}
}

func decode(payload []byte) (Wire, bool) {
	var w Wire
	if err := json.Unmarshal(payload, &w); err != nil {
		return Wire{}, false
	}
	return w, true
}
