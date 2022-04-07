//go:build integration

package transport

import (
	"fmt"
	"testing"
	"time"

	"github.com/mmatczuk/scylla-go-driver/frame"
	. "github.com/mmatczuk/scylla-go-driver/frame/response"
)

const awaitingChanges = 100 * time.Millisecond

func compareNodes(c *Cluster, addr string, expected *Node) error {
	m := c.Topology().peers
	got, ok := m[addr]
	switch {
	case !ok:
		return fmt.Errorf("couldn't find node: %s in cluster's nodes", addr)
	case got.Status() != expected.Status():
		return fmt.Errorf("got status: %t, expected: %t", got.Status(), expected.Status())
	case got.addr != expected.addr:
		return fmt.Errorf("got IP address: %s, expected: %s", got.addr, expected.addr)
	case got.rack != expected.rack:
		return fmt.Errorf("got rack name: %s, expected: %s", got.rack, expected.rack)
	case got.datacenter != expected.datacenter:
		return fmt.Errorf("got DC name: %s, expected: %s", got.datacenter, expected.datacenter)
	default:
		return nil
	}
}

func TestClusterIntegration(t *testing.T) {
	addr := frame.Inet{
		IP:   []byte{192, 168, 100, 100},
		Port: 9042,
	}

	// There is no one listening at the first address, it just checks cluster proper behavior.
	c, err := NewCluster(dummyConnConfig, []string{frame.StatusChange}, "123.123.123.123:1234", TestHost+":9042")
	if err != nil {
		t.Fatal(err)
	}

	// Checks cluster behavior after receiving event with error.
	c.handleEvent(response{Err: fmt.Errorf("fake error")})

	expected := &Node{
		addr:       TestHost,
		datacenter: "datacenter1",
		rack:       "rack1",
	}
	expected.setStatus(statusUP)

	// Checks if TestHost is present in cluster with correct attributes.
	if err := compareNodes(c, TestHost, expected); err != nil {
		t.Fatalf(err.Error())
	}

	c.handleEvent(response{
		Response: &StatusChange{
			Status:  frame.Down,
			Address: addr,
		},
	})
	expected.setStatus(statusDown)

	time.Sleep(awaitingChanges)
	// Checks if TestHost's status was updated.
	if err := compareNodes(c, TestHost, expected); err != nil {
		t.Fatalf(err.Error())
	}

	// There should be at least system keyspaces present.
	if len(c.topology.Load().(*topology).keyspaces) == 0 {
		t.Fatalf("Keyspaces failed to load")
	}

	c.handleEvent(response{
		Response: &TopologyChange{
			Change:  frame.NewNode,
			Address: addr,
		},
	})

	time.Sleep(awaitingChanges)
	// Checks if cluster can handle (fake) topology change.
	if err := compareNodes(c, TestHost, expected); err != nil {
		t.Fatalf(err.Error())
	}

	time.Sleep(awaitingChanges)
	c.Close()
	time.Sleep(awaitingChanges)
}
