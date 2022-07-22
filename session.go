package scylla

import (
	"context"
	"fmt"
	"log"

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
	errNoConnection = fmt.Errorf("no working connection")
)

type Compression = frame.Compression

var (
	Snappy Compression = frame.Snappy
	Lz4    Compression = frame.Lz4
)

type SessionConfig struct {
	Hosts  []string
	Events []EventType
	Policy transport.HostSelectionPolicy
	transport.ConnConfig
}

func DefaultSessionConfig(keyspace string, hosts ...string) SessionConfig {
	return SessionConfig{
		Hosts:      hosts,
		Policy:     transport.NewTokenAwarePolicy(""),
		ConnConfig: transport.DefaultConnConfig(keyspace),
	}
}

func (cfg SessionConfig) Clone() SessionConfig {
	v := cfg

	v.Hosts = make([]string, len(cfg.Hosts))
	copy(v.Hosts, cfg.Hosts)

	v.Events = make([]EventType, len(cfg.Events))
	copy(v.Events, cfg.Events)

	v.TLSConfig = v.TLSConfig.Clone()

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
	return nil
}

type Session struct {
	cfg     SessionConfig
	cluster *transport.Cluster
	policy  transport.HostSelectionPolicy
}

func NewSession(ctx context.Context, cfg SessionConfig) (*Session, error) {
	cfg = cfg.Clone()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cluster, err := transport.NewCluster(ctx, cfg.ConnConfig, cfg.Policy, cfg.Events, cfg.Hosts...)
	if err != nil {
		return nil, err
	}

	s := &Session{
		cfg:     cfg,
		cluster: cluster,
		policy:  cfg.Policy,
	}

	return s, nil
}

func (s *Session) Query(content string) Query {
	return Query{session: s,
		stmt: transport.Statement{Content: content, Consistency: s.cfg.DefaultConsistency},
		exec: func(ctx context.Context, conn *transport.Conn, stmt transport.Statement, pagingState frame.Bytes) (transport.QueryResult, error) {
			return conn.Query(ctx, stmt, pagingState)
		},
		asyncExec: func(ctx context.Context, conn *transport.Conn, stmt transport.Statement, pagingState frame.Bytes, handler transport.ResponseHandler) {
			conn.AsyncQuery(ctx, stmt, pagingState, handler)
		},
	}
}

func (s *Session) Prepare(ctx context.Context, content string) (Query, error) {
	n := s.policy.Node(s.cluster.NewQueryInfo(), 0)
	conn := n.LeastBusyConn()
	if conn == nil {
		return Query{}, errNoConnection
	}

	stmt := transport.Statement{Content: content, Consistency: frame.ALL}
	res, err := conn.Prepare(ctx, stmt)

	return Query{session: s,
		stmt: res,
		exec: func(ctx context.Context, conn *transport.Conn, stmt transport.Statement, pagingState frame.Bytes) (transport.QueryResult, error) {
			return conn.Execute(ctx, stmt, pagingState)
		},
		asyncExec: func(ctx context.Context, conn *transport.Conn, stmt transport.Statement, pagingState frame.Bytes, handler transport.ResponseHandler) {
			conn.AsyncExecute(ctx, stmt, pagingState, handler)
		},
	}, err
}

func (s *Session) NewTokenAwarePolicy() transport.HostSelectionPolicy {
	return transport.NewTokenAwarePolicy("")
}

func (s *Session) NewTokenAwareDCAwarePolicy(localDC string) transport.HostSelectionPolicy {
	return transport.NewTokenAwarePolicy(localDC)
}

func (s *Session) Close() {
	log.Println("session: close")
	s.cluster.Close()
}
