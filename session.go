package scylla

import (
	"fmt"
	"time"

	"github.com/mmatczuk/scylla-go-driver/frame"
	"github.com/mmatczuk/scylla-go-driver/transport"
)

// TODO: Add retry policy.
// TODO: Add Query Paging.

type EventType = string

const (
	TopologyChange EventType = "TOPOLOGY_CHANGE"
	StatusChange   EventType = "STATUS_CHANGE"
	SchemaChange   EventType = "SCHEMA_CHANGE"
)

type Consistency = uint16

const (
	ANY         Consistency = 0x0000
	ONE         Consistency = 0x0001
	TWO         Consistency = 0x0002
	THREE       Consistency = 0x0003
	QUORUM      Consistency = 0x0004
	ALL         Consistency = 0x0005
	LOCALQUORUM Consistency = 0x0006
	EACHQUORUM  Consistency = 0x0007
	SERIAL      Consistency = 0x0008
	LOCALSERIAL Consistency = 0x0009
	LOCALONE    Consistency = 0x000A
)

const (
	RoundRobin = iota
	TokenAware
	DCAware
)

var (
	ErrNoHosts   = fmt.Errorf("error in session config: no hosts given")
	ErrEventType = fmt.Errorf("error in session config: invalid event\npossible events:\n" +
		"TopologyChange EventType = \"TOPOLOGY_CHANGE\"\n" +
		"StatusChange   EventType = \"STATUS_CHANGE\"\n" +
		"SchemaChange   EventType = \"SCHEMA_CHANGE\"")
	ErrConsistency = fmt.Errorf("error in session config: invalid consistency\npossible consistencies are:\n" +
		"ANY         Consistency = 0x0000\n" +
		"ONE         Consistency = 0x0001\n" +
		"TWO         Consistency = 0x0002\n" +
		"THREE       Consistency = 0x0003\n" +
		"QUORUM      Consistency = 0x0004\n" +
		"ALL         Consistency = 0x0005\n" +
		"LOCALQUORUM Consistency = 0x0006\n" +
		"EACHQUORUM  Consistency = 0x0007\n" +
		"SERIAL      Consistency = 0x0008\n" +
		"LOCALSERIAL Consistency = 0x0009\n" +
		"LOCALONE    Consistency = 0x000A")
	errNoConnection        = fmt.Errorf("no working connection")
	errHostSelectionPolicy = fmt.Errorf("error in session config: invalid host selection policy\npossible policies are:" +
		"RoundRobin\n" +
		"TokenAware\n" +
		"DCAware")
)

func DefaultSessionConfig(keyspace string, hosts ...string) SessionConfig {
	return SessionConfig{
		Hosts:  hosts,
		Policy: TokenAware,
		ConnConfig: transport.ConnConfig{
			Keyspace:           keyspace,
			Timeout:            500 * time.Millisecond,
			TCPNoDelay:         true,
			DefaultConsistency: LOCALQUORUM,
		},
	}
}

type SessionConfig struct {
	Hosts   []string
	Events  []EventType
	Policy  int
	LocalDC string
	transport.ConnConfig
}

func (cfg SessionConfig) Clone() SessionConfig {
	v := cfg

	v.Hosts = make([]string, len(cfg.Hosts))
	copy(v.Hosts, cfg.Hosts)

	v.Events = make([]EventType, len(cfg.Events))
	copy(v.Events, cfg.Events)

	return v
}

func (cfg *SessionConfig) Validate() error {
	if len(cfg.Hosts) == 0 {
		return ErrNoHosts
	}
	for _, e := range cfg.Events {
		if e != TopologyChange && e != StatusChange && e != SchemaChange {
			return ErrEventType
		}
	}
	if cfg.DefaultConsistency > LOCALONE {
		return ErrConsistency
	}
	if cfg.Policy != RoundRobin && cfg.Policy != TokenAware && cfg.Policy != DCAware {
		return errHostSelectionPolicy
	}
	return nil
}

func (cfg SessionConfig) pickHostSelectionPolicy() transport.HostSelectionPolicy {
	switch cfg.Policy {
	case RoundRobin:
		p := transport.NewRoundRobinPolicy()
		return &p
	case TokenAware:
		rr := transport.NewRoundRobinPolicy()
		p := transport.NewTokenAwarePolicy(&rr)
		return &p
	case DCAware:
		p := transport.NewDCAwareRoundRobin(cfg.LocalDC)
		return &p
	default:
		p := transport.NewRoundRobinPolicy()
		return &p
	}
}

type Session struct {
	cfg     SessionConfig
	cluster *transport.Cluster
	hsp     transport.HostSelectionPolicy
}

func NewSession(cfg SessionConfig) (*Session, error) {
	cfg = cfg.Clone()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cluster, err := transport.NewCluster(cfg.ConnConfig, cfg.Events, cfg.Hosts...)
	if err != nil {
		return nil, err
	}

	s := &Session{
		cfg:     cfg,
		cluster: cluster,
		hsp:     cfg.pickHostSelectionPolicy(),
	}

	return s, nil
}

func (s *Session) Query(content string) Query {
	return Query{session: s,
		stmt: transport.Statement{Content: content, Consistency: s.cfg.DefaultConsistency},
		exec: func(conn *transport.Conn, stmt transport.Statement, pagingState frame.Bytes) (transport.QueryResult, error) {
			return conn.Query(stmt, pagingState)
		},
	}
}

func (s *Session) Prepare(content string) (Query, error) {
	it := s.hsp.PlanIter(s.cluster.NewQueryInfo())
	conn := it().LeastBusyConn()
	if conn == nil {
		return Query{}, errNoConnection
	}

	stmt := transport.Statement{Content: content, Consistency: s.cfg.DefaultConsistency}
	res, err := conn.Prepare(stmt)

	return Query{session: s,
		stmt: res,
		exec: func(conn *transport.Conn, stmt transport.Statement, pagingState frame.Bytes) (transport.QueryResult, error) {
			return conn.Execute(stmt, pagingState)
		},
	}, err
}

func (s *Session) Close() {
	s.cluster.Close()
}
