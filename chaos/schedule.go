// Package chaos scripts partition, heal and crash faults at fixed virtual
// milliseconds so a failure scenario is data instead of code.
//
// A Scheduler is bound to an engine through Config.OnEvent; it applies every
// fault whose time has been reached, in script order.
package chaos

import (
	"fmt"
	"sort"

	sim "quorum-sim"
)

// Kind is a scripted fault type.
type Kind int

const (
	// Partition cuts the link between A and B in both directions.
	Partition Kind = iota
	// Heal restores the link between A and B.
	Heal
	// Crash stops node A permanently.
	Crash
)

// Fault fires at virtual millisecond At.
type Fault struct {
	At   int64
	Kind Kind
	A, B sim.NodeID
}

// Scheduler holds a validated, time-ordered fault script.
type Scheduler struct {
	faults []Fault
	next   int
}

// New copies and sorts faults by time, stable within equal times, then
// validates that each entry has the endpoints its kind needs.
func New(faults []Fault) (*Scheduler, error) {
	script := append([]Fault(nil), faults...)
	for i, f := range script {
		if f.At < 0 {
			return nil, fmt.Errorf("chaos: fault %d has negative time %d", i, f.At)
		}
		switch f.Kind {
		case Partition, Heal:
			if f.A == "" || f.B == "" || f.A == f.B {
				return nil, fmt.Errorf("chaos: fault %d needs two distinct endpoints", i)
			}
		case Crash:
			if f.A == "" {
				return nil, fmt.Errorf("chaos: crash fault %d has no node", i)
			}
		default:
			return nil, fmt.Errorf("chaos: fault %d has unknown kind %d", i, f.Kind)
		}
	}
	sort.SliceStable(script, func(i, j int) bool { return script[i].At < script[j].At })
	return &Scheduler{faults: script}, nil
}

// Hook returns the callback to install as Config.OnEvent.
func (s *Scheduler) Hook() func(*sim.Engine) {
	return func(e *sim.Engine) { s.Apply(e) }
}

// Apply fires every fault due at the engine's current virtual time.
func (s *Scheduler) Apply(e *sim.Engine) {
	if e == nil {
		return
	}
	for s.next < len(s.faults) && s.faults[s.next].At <= e.Time() {
		f := s.faults[s.next]
		s.next++
		switch f.Kind {
		case Partition:
			e.Partition(f.A, f.B)
		case Heal:
			e.Heal(f.A, f.B)
		case Crash:
			e.Stop(f.A)
		}
	}
}

// Remaining reports how many faults have not fired yet.
func (s *Scheduler) Remaining() int { return len(s.faults) - s.next }
