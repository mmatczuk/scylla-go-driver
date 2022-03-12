//go:build integration

package transport

import (
	"fmt"
	"testing"
	"time"

	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/response"
)

const awaitingChanges = 250 * time.Millisecond

func compareNodes(c *Cluster, addr string, expected *Node) error {
	m := c.GetPeers()
	got, ok := m[addr]
	switch {
	case !ok:
		return fmt.Errorf("couldn't find node: %s in cluster's nodes", addr)
	case got.status.Load() != expected.status.Load():
		return fmt.Errorf("got status: %t, expected: %t", got.status.Load(), expected.status.Load())
	case got.addr != expected.addr:
		return fmt.Errorf("got IP address: %s, expected: %s", got.addr, got.addr)
	case got.rack != expected.rack:
		return fmt.Errorf("got rack name: %s, expected: %s", got.rack, expected.rack)
	case got.datacenter != expected.datacenter:
		return fmt.Errorf("got DC name: %s, expected: %s", got.datacenter, expected.datacenter)
	default:
		return nil
	}
}

func TestClusterIntegration(t *testing.T) {
	cfg := ConnConfig{Timeout: 250 * time.Millisecond}
	addr := frame.Inet{
		IP:   []byte{192, 168, 100, 100},
		Port: 9042,
	}

	// There is no one listening at the first address, it just checks cluster proper behavior.
	c, err := NewCluster(cfg, []string{frame.StatusChange}, "123.123.123.123:1234", TestHost+":9042")
	if err != nil {
		t.Fatal(err)
	}

	expected := &Node{
		addr:       TestHost,
		datacenter: "datacenter1",
		rack:       "rack1",
	}
	expected.status.Store(statusUP)

	// Checks if TestHost is present in cluster with correct attributes.
	if err = compareNodes(c, TestHost, expected); err != nil {
		t.Fatalf(err.Error())
	}

	c.events <- response{
		Response: &StatusChange{
			Status:  frame.Down,
			Address: addr,
		},
	}
	expected.status.Store(statusDown)

	time.Sleep(awaitingChanges)
	// Checks if TestHost's status was updated.
	if err = compareNodes(c, TestHost, expected); err != nil {
		t.Fatalf(err.Error())
	}

	c.events <- response{
		Response: &TopologyChange{
			Change:  frame.NewNode,
			Address: addr,
		},
	}

	time.Sleep(awaitingChanges)
	// Checks if cluster can handle (fake) topology change.
	if err = compareNodes(c, TestHost, expected); err != nil {
		t.Fatalf(err.Error())
	}

	time.Sleep(awaitingChanges)

	c.StopCluster()

	time.Sleep(awaitingChanges)
}