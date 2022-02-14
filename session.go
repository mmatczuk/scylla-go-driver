package main

import (
	"scylla-go-driver/transport"
)

type Session struct {
	cluster *transport.Cluster
	control *transport.ControlConn
	cfg     transport.ConnConfig
}

func OpenSession(addr string, cfg transport.ConnConfig) (*Session, error) {
	cluster := &transport.Cluster{}
	control, err := transport.OpenControlConn(addr, cluster, cfg)
	if err != nil {
		return nil, err
	}

	return &Session{
		cluster: cluster,
		control: control,
		cfg:     cfg,
	}, nil
}
