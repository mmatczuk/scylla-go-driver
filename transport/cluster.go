package transport

import (
	"fmt"
	"net"
	"time"

	"scylla-go-driver/frame"
)

// Should it be in the same var?
var (
	PeerQuery = Statement{
		Content:     "SELECT * FROM system.peers",
		Consistency: frame.ONE,
	}

	refreshTime = time.Second * 60
)

const (
	peerAddr = 0
	peerDC   = 1
	peerRack = 4
)

type Cluster struct {
	Data    *ClusterData
	Control *ControlConn

	// chan for refreshing?
}

type DataCenter struct {
	Nodes   []Node
	RackCnt uint // Type?
}

type ClusterData struct {
	Peers map[string]*Node // Difference between known peers and all nodes?

	// Yet to be used.
	// Ring        map[Token]*Node
	// DataCenters map[string]*DataCenter
}

type ControlConn struct {
	Cld          *ClusterData
	Conn         *Conn
	Cfg          ConnConfig
	RefreshTimer *time.Ticker

	// pool config?
	// chan for refreshing (connected with the one in cluster)?
}

func OpenControlConn(addr string, cld *ClusterData, cfg ConnConfig) (*ControlConn, error) {
	c, err := OpenConn(addr, nil, cfg)
	if err != nil {
		return nil, fmt.Errorf("opening control conn: %w", err)
	}

	control := &ControlConn{
		Cld:          cld,
		Conn:         c,
		Cfg:          cfg,
		RefreshTimer: time.NewTicker(refreshTime),
	}

	err = control.UpdateTopology()
	go control.loop()

	return control, err
}

func (c *ControlConn) UpdateTopology() error {
	res, err := c.Conn.Query(PeerQuery, nil)
	if err != nil {
		return fmt.Errorf("discovering topology: %w", err)
	}

	// Should we check if all peers are present in the current
	// map instead of creating a new one?
	m := map[string]*Node{}
	for _, v := range res.Rows {
		addr := net.IP(v[peerAddr]).String()
		m[addr] = &Node{
			addr:       addr,
			datacenter: string(v[peerDC]),
			rack:       string(v[peerRack]),
			pool:       InitNodeConnPool(addr, c.Cfg),
		}
	}

	c.Cld.Peers = m
	return nil
}

func (c *ControlConn) loop() {
	for {
		<-c.RefreshTimer.C
		if err := c.UpdateTopology(); err != nil {
			// Handling error?
			return
		}
	}
}
