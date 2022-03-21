//go:build integration

package transport

import (
	"math"
	"testing"
	"time"
)

const refillerBackoff = 500 * time.Millisecond

func TestConnPoolIntegration(t *testing.T) {
	p, err := NewConnPool(TestHost, ConnConfig{})
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Wait for refiller to fill connections to shards")
	time.Sleep(refillerBackoff)

	t.Log("Check if connections to shards are established")
	for i, c := range p.AllConns() {
		if c == nil {
			t.Fatalf("no conn for shard %d", i)
		}
	}

	t.Log("Close connections")
	for _, c := range p.AllConns() {
		c.Close()
	}

	t.Log("Wait for refiller to fill connections to shards")
	time.Sleep(refillerBackoff)

	t.Log("Check if connections have been refilled")
	for i, c := range p.AllConns() {
		if c == nil {
			t.Fatalf("no conn for shard %d", i)
		}
	}

	t.Log("Close pool")
	p.Close()
	time.Sleep(refillerBackoff)

	t.Log("Check if connections are closed")
	for _, c := range p.AllConns() {
		if c != nil {
			t.Fatalf("conn %s", c)
		}
	}
}

func TestConnPoolConn(t *testing.T) {
	p, err := NewConnPool(TestHost, ConnConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	t.Log("Wait for refiller to fill connections to shards")
	time.Sleep(refillerBackoff)

	t.Log("Check if connections to shards are established")
	for i, c := range p.AllConns() {
		if c == nil {
			t.Fatalf("no conn for shard %d", i)
		}
	}

	t0 := MurmurToken([]byte(""))
	if conn := p.Conn(t0); conn == nil || conn.Shard() != 0 {
		t.Fatal("invalid return of Conn")
	}

	load := uint32(math.Floor(maxStreamID*heavyLoadThreshold + 1))
	p.Conn(t0).metrics.InQueue.Store(load)

	if conn := p.Conn(t0); conn == nil {
		t.Fatal("invalid return of Conn")
	} else if conn.Shard() == 0 {
		t.Fatalf("invalid load distribution")
	}

	t1 := MurmurToken([]byte("0")) // Very big number approx. 3 * 10^18.
	if conn := p.Conn(t1); conn == nil {
		t.Fatal("invalid return of Conn")
	}
}
