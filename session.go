package main // Probably not the best name.

import (
	"fmt"
	"time"

	"scylla-go-driver/frame"
	"scylla-go-driver/transport"
)

type Session struct {
	cluster            *transport.Cluster
	defaultConsistency frame.Consistency
}

type SessionConfig struct {
	hosts []string // List of hosts given by user at the beginning.

	tcpNoDelay         bool
	timeout            time.Duration
	defaultConsistency frame.Consistency
}

// newConnConfig creates ConnConfig with the same configuration as in SessionConfig.
func (s SessionConfig) newConnConfig() transport.ConnConfig {
	return transport.ConnConfig{
		TCPNoDelay:         s.tcpNoDelay,
		Timeout:            s.timeout,
		DefaultConsistency: s.defaultConsistency,
	}
}

// Connect creates session and discovers cluster topology via control connection.
// nolint:unused // This will be used.
func Connect(cfg SessionConfig) (*Session, error) {
	if len(cfg.hosts) == 0 {
		return nil, fmt.Errorf("connecting session requireas at least 1 host address")
	}

	connCfg := cfg.newConnConfig()
	cld := &transport.ClusterData{}

	for _, v := range cfg.hosts {
		if control, err := transport.OpenControlConn(v, cld, connCfg); err == nil {
			return &Session{
				cluster: &transport.Cluster{
					Data:    cld,
					Control: control,
				},
				defaultConsistency: cfg.defaultConsistency,
			}, nil
		}
	}

	return nil, fmt.Errorf("couldn't open control connection to any host")
}
