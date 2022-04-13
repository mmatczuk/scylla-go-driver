package transport

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/mmatczuk/scylla-go-driver/frame"
	. "github.com/mmatczuk/scylla-go-driver/frame/response"

	"github.com/google/btree"
	"go.uber.org/atomic"
)

type (
	peerMap    = map[string]*Node
	dcRacksMap = map[string]int
	dcRFMap    = map[string]uint32
	ksMap      = map[string]keyspace

	requestChan chan struct{}
)

const BTreeDegree = 2

type Cluster struct {
	topology          atomic.Value // *topology
	control           *Conn
	cfg               ConnConfig
	handledEvents     []frame.EventType // This will probably be moved to config.
	knownHosts        []string
	refreshChan       requestChan
	reopenControlChan requestChan
	closeChan         requestChan
}

type topology struct {
	peers     peerMap
	dcRacks   dcRacksMap
	nodes     []*Node
	ring      *btree.BTree[RingEntry]
	keyspaces ksMap
}

// TODO: mmt this is a hack and must be removed.
func (t *topology) PeerHACK() *Node {
	for _, p := range t.peers {
		return p
	}
	panic("no nodes")
}

type keyspace struct {
	strategy strategy
	// TODO: Add and use attributes below.
	// tables		      map[string]table
	// user_defined_types map[string](string, cqltype)
}

type strategyClass string

// JCN stands for Java Class Name.
const (
	networkTopologyStrategyJCN strategyClass = "org.apache.cassandra.locator.NetworkTopologyStrategy"
	simpleStrategyJCN          strategyClass = "org.apache.cassandra.locator.SimpleStrategy"
	localStrategyJCN           strategyClass = "org.apache.cassandra.locator.LocalStrategy"
	networkTopologyStrategy    strategyClass = "NetworkTopologyStrategy"
	simpleStrategy             strategyClass = "SimpleStrategy"
	localStrategy              strategyClass = "LocalStrategy"
)

type strategy struct {
	class strategyClass
	rf    uint32            // Used in simpleStrategy.
	dcRF  dcRFMap           // Used in networkTopologyStrategy.
	data  map[string]string // Used in other strategy.

}

// QueryInfo represents data required for host selection policy to create query plan.
// Token and strategy are only necessary for token aware policies.
type QueryInfo struct {
	tokenAwareness bool
	token          Token
	topology       *topology
	strategy       strategy
}

func (c *Cluster) NewQueryInfo() QueryInfo {
	return QueryInfo{
		tokenAwareness: false,
		topology:       c.Topology(),
	}
}

func (c *Cluster) NewTokenAwareQueryInfo(t Token, ks string) (QueryInfo, error) {
	top := c.Topology()
	// When keyspace is not specified, we take default keyspace from ConnConfig.
	if ks == "" {
		ks = c.cfg.Keyspace
	}
	if stg, ok := top.keyspaces[ks]; ok {
		return QueryInfo{
			tokenAwareness: true,
			token:          t,
			topology:       top,
			strategy:       stg.strategy,
		}, nil
	} else {
		return QueryInfo{}, fmt.Errorf("couldn't find given keyspace in current topology")
	}
}

// replicas return slice of nodes (desirably of length cnt) holding data described by token.
// filter function allows applying additional requirements for nodes to be taken.
func (t *topology) replicas(token Token, size int, filter func(*Node, []*Node) bool) []*Node {
	res := make([]*Node, 0, size)
	it := func(i RingEntry) bool {
		n := i.node
		if filter(n, res) {
			res = append(res, n)
		}
		return len(res) < size
	}

	re := RingEntry{token: token}
	// Token ring has cyclic architecture, so we also have to
	// get back to the beginning after reaching its end.
	t.ring.AscendGreaterOrEqual(re, it)
	if len(res) < size {
		t.ring.AscendLessThan(re, it)
	}
	return res
}

// NewCluster also creates control connection and starts handling events and refreshing topology.
func NewCluster(cfg ConnConfig, e []frame.EventType, hosts ...string) (*Cluster, error) {
	c := &Cluster{
		cfg:               cfg,
		handledEvents:     e,
		knownHosts:        hosts,
		refreshChan:       make(requestChan, 1),
		reopenControlChan: make(requestChan, 1),
		closeChan:         make(requestChan, 1),
	}
	c.setTopology(&topology{})

	if control, err := c.NewControl(); err != nil {
		return nil, fmt.Errorf("create control connection: %w", err)
	} else {
		c.control = control
	}
	if err := c.refreshTopology(); err != nil {
		return nil, fmt.Errorf("refresh topology: %w", err)
	}

	go c.loop()
	return c, nil
}

func (c *Cluster) NewControl() (*Conn, error) {
	log.Printf("cluster: open control connection")
	errs := make([]string, 0)
	for _, addr := range c.knownHosts {
		conn, err := OpenConn(addr, nil, c.cfg)
		if err == nil {
			if err := conn.RegisterEventHandler(c.handleEvent, c.handledEvents...); err == nil {
				return conn, nil
			} else {
				log.Printf("cluster: open control connection: node %s failed to register for events: %v", conn, err)
				errs = append(errs, fmt.Sprintf("node %s failed to register for events: %v", conn, err))
			}
		} else {
			log.Printf("cluster: open control connection: node %s failed to connect: %v", addr, err)
			errs = append(errs, fmt.Sprintf("node %s failed to connect: %v", addr, err))
		}
		if conn != nil {
			conn.Close()
		}
	}

	return nil, fmt.Errorf("couldn't open control connection to any known host:\n%s", strings.Join(errs, "\n"))
}

// refreshTopology creates new topology filled with the result of keyspaceQuery, localQuery and peerQuery.
// Old topology is replaced with the new one atomically to prevent dirty reads.
func (c *Cluster) refreshTopology() error {
	log.Printf("cluster: refresh topology")
	rows, err := c.getAllNodesInfo()
	if err != nil {
		return fmt.Errorf("query info about nodes in cluster: %w", err)
	}

	old := c.Topology().peers
	t := newTopology()
	t.keyspaces, err = c.updateKeyspace()
	if err != nil {
		return fmt.Errorf("query keyspaces: %w", err)
	}

	type uniqueRack struct {
		dc   string
		rack string
	}
	u := make(map[uniqueRack]bool)

	for _, r := range rows {
		n, err := parseNodeFromRow(r)
		if err != nil {
			return err
		}
		// If node is present in both maps we can reuse its connection pool.
		if node, ok := old[n.addr]; ok {
			n.pool = node.pool
			n.setStatus(node.Status())
		} else {
			if pool, err := NewConnPool(n.addr, c.cfg); err != nil {
				n.setStatus(statusDown)
			} else {
				n.setStatus(statusUP)
				n.pool = pool
			}
		}

		t.peers[n.addr] = n
		t.nodes = append(t.nodes, n)
		u[uniqueRack{dc: n.datacenter, rack: n.rack}] = true
		if err := parseTokensFromRow(n, r, t.ring); err != nil {
			return err
		}
	}
	// Counts unique racks in data centers.
	for k := range u {
		t.dcRacks[k.dc]++
	}
	// We want to close pools of nodes present in previous and absent in current topology.
	for k, v := range old {
		if _, ok := t.peers[k]; v.pool != nil && !ok {
			v.pool.Close()
		}
	}

	c.setTopology(t)
	drainChan(c.refreshChan)
	return nil
}

func newTopology() *topology {
	return &topology{
		peers:   make(peerMap),
		dcRacks: make(dcRacksMap),
		nodes:   make([]*Node, 0),
		ring:    btree.New[RingEntry](BTreeDegree),
	}
}

var (
	peerQuery = Statement{
		Content:     "SELECT peer, data_center, rack, tokens FROM system.peers",
		Consistency: frame.ONE,
	}

	localQuery = Statement{
		Content:     "SELECT rpc_address, data_center, rack, tokens FROM system.local",
		Consistency: frame.ONE,
	}

	keyspaceQuery = Statement{
		Content:     "SELECT keyspace_name, replication FROM system_schema.keyspaces",
		Consistency: frame.ONE,
	}
)

func (c *Cluster) getAllNodesInfo() ([]frame.Row, error) {
	peerRes, err := c.control.Query(peerQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("discover peer topology: %w", err)
	}

	localRes, err := c.control.Query(localQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("discover local topology: %w", err)
	}

	return append(peerRes.Rows, localRes.Rows[0]), nil
}

func parseNodeFromRow(r frame.Row) (*Node, error) {
	const (
		addrIndex = 0
		dcIndex   = 1
		rackIndex = 2
	)
	addr, err := r[addrIndex].AsIP()
	if err != nil {
		return nil, fmt.Errorf("addr column: %w", err)
	}
	dc, err := r[dcIndex].AsText()
	if err != nil {
		return nil, fmt.Errorf("datacenter column: %w", err)
	}
	rack, err := r[rackIndex].AsText()
	if err != nil {
		return nil, fmt.Errorf("rack column: %w", err)
	}
	return &Node{
		addr:       addr.String(),
		datacenter: dc,
		rack:       rack,
	}, nil
}

func (c *Cluster) updateKeyspace() (ksMap, error) {
	const ksNameIndex = 0
	rows, err := c.control.Query(keyspaceQuery, nil)
	if err != nil {
		return nil, err
	}
	res := make(ksMap, len(rows.Rows))
	for _, r := range rows.Rows {
		name, err := r[ksNameIndex].AsText()
		if err != nil {
			return nil, fmt.Errorf("keyspace name column: %w", err)
		}
		stg, err := parseStrategyFromRow(r)
		if err != nil {
			return nil, fmt.Errorf("keyspace replication column: %w", err)
		}
		res[name] = keyspace{strategy: stg}
	}
	return res, nil
}

func parseStrategyFromRow(r frame.Row) (strategy, error) {
	const replicationIndex = 1
	stg, err := r[replicationIndex].AsStringMap()
	if err != nil {
		return strategy{}, fmt.Errorf("strategy and rf column: %w", err)
	}
	className, ok := stg["class"]
	if !ok {
		return strategy{}, fmt.Errorf("strategy map should have a 'class' field")
	}
	delete(stg, "class")
	// We set strategy name to its shorter version.
	switch strategyClass(className) {
	case simpleStrategyJCN, simpleStrategy:
		return parseSimpleStrategy(simpleStrategy, stg)
	case networkTopologyStrategyJCN, networkTopologyStrategy:
		return parseNetworkStrategy(networkTopologyStrategy, stg)
	case localStrategyJCN, localStrategy:
		return strategy{
			class: localStrategy,
			rf:    1,
		}, nil
	default:
		return strategy{
			class: strategyClass(className),
			data:  stg,
		}, nil
	}
}

func parseSimpleStrategy(name strategyClass, stg map[string]string) (strategy, error) {
	rfStr, ok := stg["replication_factor"]
	if !ok {
		return strategy{}, fmt.Errorf("replication_factor field not found")
	}
	rf, err := strconv.ParseUint(rfStr, 10, 32)
	if err != nil {
		return strategy{}, fmt.Errorf("could not parse replication factor as unsigned int")
	}
	return strategy{
		class: name,
		rf:    uint32(rf),
	}, nil
}

func parseNetworkStrategy(name strategyClass, stg map[string]string) (strategy, error) {
	dcRF := make(dcRFMap, len(stg))
	for dc, v := range stg {
		rf, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return strategy{}, fmt.Errorf("could not parse replication factor as int")
		}
		dcRF[dc] = uint32(rf)
	}
	return strategy{
		class: name,
		dcRF:  dcRF,
	}, nil
}

// parseTokensFromRow also inserts tokens into ring.
func parseTokensFromRow(n *Node, r frame.Row, ring *btree.BTree[RingEntry]) error {
	const tokensIndex = 3
	if tokens, err := r[tokensIndex].AsStringSlice(); err != nil {
		return err
	} else {
		for _, t := range tokens {
			if v, err := strconv.ParseInt(t, 10, 64); err != nil {
				return fmt.Errorf("couldn't parse token string: %w", err)
			} else {
				ring.ReplaceOrInsert(RingEntry{
					node:  n,
					token: Token(v),
				})
			}
		}
	}
	return nil
}

func (c *Cluster) Topology() *topology {
	return c.topology.Load().(*topology)
}

func (c *Cluster) setTopology(t *topology) {
	c.topology.Store(t)
}

// handleEvent creates function which is passed to control connection
// via registerEvents in order to handle events right away instead
// of registering handlers for them.
func (c *Cluster) handleEvent(r response) {
	if r.Err != nil {
		log.Printf("cluster: received event with error: %v", r.Err)
		c.RequestReopenControl()
		return
	}
	switch v := r.Response.(type) {
	case *TopologyChange:
		c.handleTopologyChange(v)
	case *StatusChange:
		c.handleStatusChange(v)
	case *SchemaChange:
		// TODO: add schema change.
	default:
		log.Printf("cluster: unsupported event type: %v", r.Response)
	}
}

func (c *Cluster) handleTopologyChange(v *TopologyChange) {
	log.Printf("cluster: handle topology change: %+#v", v)
	c.RequestRefresh()
}

func (c *Cluster) handleStatusChange(v *StatusChange) {
	log.Printf("cluster: handle status change: %+#v", v)
	m := c.Topology().peers
	addr := v.Address.String()
	if n, ok := m[addr]; ok {
		switch v.Status {
		case frame.Up:
			n.setStatus(statusUP)
		case frame.Down:
			n.setStatus(statusDown)
		default:
			log.Printf("cluster: status change not supported: %+#v", v)
		}
	} else {
		log.Printf("cluster: unknown node %s received status change: %+#v in topology %v", addr, v, m)
		c.RequestRefresh()
	}
}

const refreshInterval = 60 * time.Second

// loop handles cluster requests.
func (c *Cluster) loop() {
	ticker := time.NewTicker(refreshInterval)
	for {
		select {
		case <-c.refreshChan:
			c.tryRefresh()
		case <-c.reopenControlChan:
			c.tryReopenControl()
		case <-c.closeChan:
			c.handleClose()
			return
		case <-ticker.C:
			c.tryRefresh()
		}
	}
}

const tryRefreshInterval = time.Second

// tryRefresh refreshes cluster topology.
// In case of error tries to reopen control connection and tries again.
func (c *Cluster) tryRefresh() {
	if err := c.refreshTopology(); err != nil {
		c.RequestReopenControl()
		time.AfterFunc(tryRefreshInterval, c.RequestRefresh)
		log.Printf("cluster: refresh topology: %v", err)
	}
}

const tryReopenControlInterval = time.Second

func (c *Cluster) tryReopenControl() {
	log.Printf("cluster: reopen control connection")
	if control, err := c.NewControl(); err != nil {
		time.AfterFunc(tryReopenControlInterval, c.RequestReopenControl)
		log.Printf("cluster: failed to reopen control connection: %v", err)
	} else {
		c.control.Close()
		c.control = control
	}
	drainChan(c.reopenControlChan)
}

func (c *Cluster) handleClose() {
	log.Printf("cluster: handle cluster close")
	c.control.Close()
	m := c.Topology().peers
	for _, v := range m {
		if v.pool != nil {
			v.pool.Close()
		}
	}
}

func (c *Cluster) RequestRefresh() {
	log.Printf("cluster: requested to refresh cluster topology")
	select {
	case c.refreshChan <- struct{}{}:
	default:
	}
}

func (c *Cluster) RequestReopenControl() {
	log.Printf("cluster: requested to reopen control connection")
	select {
	case c.reopenControlChan <- struct{}{}:
	default:
	}
}

func (c *Cluster) Close() {
	log.Printf("cluster: requested to close cluster")
	select {
	case c.closeChan <- struct{}{}:
	default:
	}
}

func drainChan(c requestChan) {
	select {
	case <-c:
	default:
	}
}
