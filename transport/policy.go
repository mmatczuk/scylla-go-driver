package transport

import (
	"log"

	"go.uber.org/atomic"
)

// HostSelectionPolicy prepares plan (slice of Nodes) and returns iterator that goes over it.
// After going through the whole plan, iterator returns nil instead of valid *Node.
type HostSelectionPolicy interface {
	PlanIter(QueryInfo) func() *Node
}

// WrapperPolicy is used to combine round-robin policies with token aware ones.
type WrapperPolicy interface {
	HostSelectionPolicy
	WrapPlan([]*Node) func() *Node
}

// In both round-robin policies counter has to be taken modulo length of node slice.
type roundRobinPolicy struct {
	counter atomic.Int64
}

func NewRoundRobinPolicy() roundRobinPolicy {
	return roundRobinPolicy{counter: *atomic.NewInt64(0)}
}

func (p *roundRobinPolicy) PlanIter(qi QueryInfo) func() *Node {
	return p.WrapPlan(qi.topology.nodes)
}

func (p *roundRobinPolicy) WrapPlan(plan []*Node) func() *Node {
	i := int(p.counter.Inc() - 1)
	start := i
	return func() *Node {
		if i == start+len(plan) {
			return nil
		}
		defer func() { i++ }()
		return plan[i%len(plan)]
	}
}

type dcAwareRoundRobinPolicy struct {
	counter atomic.Int64
	localDC string
}

func NewDCAwareRoundRobin(dc string) dcAwareRoundRobinPolicy {
	return dcAwareRoundRobinPolicy{
		counter: *atomic.NewInt64(0),
		localDC: dc,
	}
}

func (p *dcAwareRoundRobinPolicy) PlanIter(qi QueryInfo) func() *Node {
	return p.WrapPlan(qi.topology.nodes)
}

func (p *dcAwareRoundRobinPolicy) WrapPlan(plan []*Node) func() *Node {
	i := p.counter.Inc() - 1
	l := make([]*Node, 0)
	r := make([]*Node, 0)
	for _, n := range plan {
		if p.localDC == n.datacenter {
			l = append(l, n)
		} else {
			r = append(r, n)
		}
	}
	lit := (&roundRobinPolicy{counter: *atomic.NewInt64(i)}).WrapPlan(l)
	rit := (&roundRobinPolicy{counter: *atomic.NewInt64(i)}).WrapPlan(r)
	return func() *Node {
		if n := lit(); n != nil {
			return n
		} else {
			return rit()
		}
	}
}

type tokenAwarePolicy struct {
	wrapperPolicy WrapperPolicy
}

func NewTokenAwarePolicy(wp WrapperPolicy) tokenAwarePolicy {
	return tokenAwarePolicy{wrapperPolicy: wp}
}

func (p *tokenAwarePolicy) PlanIter(qi QueryInfo) func() *Node {
	// Fallback to policy implemented in wrapperPolicy.
	if !qi.tokenAwareness {
		return p.wrapperPolicy.PlanIter(qi)
	}

	switch qi.strategy.class {
	case simpleStrategy, localStrategy:
		return p.wrapperPolicy.WrapPlan(p.simpleStrategyReplicas(qi))
	case networkTopologyStrategy:
		return p.wrapperPolicy.WrapPlan(p.networkTopologyStrategyReplicas(qi))
	default:
		log.Printf("host selection policy: query with 'other' strategy class")
		// TODO: add support for other strategies. For now fallback to wrapper.
		return p.wrapperPolicy.PlanIter(qi)
	}
}

func (p *tokenAwarePolicy) simpleStrategyReplicas(qi QueryInfo) []*Node {
	return qi.topology.replicas(qi.token, int(qi.strategy.rf), func(n *Node, res []*Node) bool {
		for _, v := range res {
			if n.addr == v.addr {
				return false
			}
		}
		return true
	})
}

func (p *tokenAwarePolicy) networkTopologyStrategyReplicas(qi QueryInfo) []*Node {
	resLen := 0
	// repeats store the amount of nodes from the same rack that we can take in given DC.
	repeats := make(map[string]int, len(qi.strategy.dcRF))
	for k, v := range qi.strategy.dcRF {
		resLen += int(v)
		repeats[k] = int(v) - qi.topology.dcRacks[k]
	}

	filter := func(n *Node, res []*Node) bool {
		rf := qi.strategy.dcRF[n.datacenter]
		fromDC := 0
		fromRack := 0
		for _, v := range res {
			if n.addr == v.addr {
				return false
			}
			if n.datacenter == v.datacenter {
				fromDC++
				if n.rack == v.rack {
					fromRack++
				}
			}
		}

		if fromDC < int(rf) {
			if fromRack == 0 {
				return true
			}
			if repeats[n.datacenter] > 0 {
				repeats[n.datacenter]--
				return true
			}
		}
		return false
	}
	return qi.topology.replicas(qi.token, resLen, filter)
}
