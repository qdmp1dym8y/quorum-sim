package sim

import "math/rand/v2"

type link struct{ a, b NodeID }

// Net injects latency, loss and symmetric partitions. All randomness comes
// from the engine seed and is consumed on a single goroutine.
type Net struct {
	rng      *rand.Rand
	min, max int64
	dropRate float64
	cutLinks map[link]bool
}

func newNet(rng *rand.Rand, min, max int64, dropRate float64) *Net {
	return &Net{rng: rng, min: min, max: max, dropRate: dropRate, cutLinks: map[link]bool{}}
}

func key(a, b NodeID) link {
	if b < a {
		a, b = b, a
	}
	return link{a, b}
}

func (n *Net) reachable(a, b NodeID) bool {
	return a == b || !n.cutLinks[key(a, b)]
}

func (n *Net) cut(a, b NodeID, blocked bool) {
	n.cutLinks[key(a, b)] = blocked
}

// send schedules o unless the link is down or the message is dropped.
// Latency is drawn even when max == min so the RNG stream stays uniform.
func (n *Net) send(e *Engine, now int64, from NodeID, o Out) {
	if !n.reachable(from, o.To) {
		return
	}
	if n.rng.Float64() < n.dropRate {
		return
	}
	lat := n.min + n.rng.Int64N(n.max-n.min+1)
	e.schedule(event{
		at:   now + lat,
		node: o.To,
		msg:  Msg{From: from, To: o.To, Payload: o.Payload, At: now + lat},
	})
}
