package transport

import (
	"github.com/google/btree"
	"go.uber.org/atomic"
)

type nodeStatus = atomic.Bool

const (
	statusDown = false
	statusUP   = true
)

type Node struct {
	addr       string
	datacenter string
	rack       string
	pool       *ConnPool
	status     nodeStatus
}

func (n *Node) Status() bool {
	return n.status.Load()
}

func (n *Node) setStatus(v bool) {
	n.status.Store(v)
}

func (n *Node) RandomConnection() *Conn {
	return n.pool.LeastBusyConn()
}

type RingEntry struct {
	node  *Node
	token Token
}

func (r RingEntry) Less(i btree.Item) bool {
	return r.token < i.(RingEntry).token
}
