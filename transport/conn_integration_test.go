//go:build integration

package transport

import (
	"sync"
	"testing"
	"time"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestOpenShardConnIntegration(t *testing.T) {
	si := ShardInfo{
		Shard:    1,
		NrShards: 2, // Note that scylla node from docker-compose has only 2 shards.
	}

	// TODO check shard info from supported
	// Note that only direct IP calls ensures correct shard mapping.
	// I tested it manually using time.sleep() and checking if connection was mapped to appropriate shard with cqlsh ("SELECT * FROM system.clients;").
	c, err := OpenShardConn("127.0.0.1:19042", si, ConnConfig{Timeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	c.close()
}

type connTestHelper struct {
	t    testing.TB
	conn *Conn
}

func newConnTestHelper(t testing.TB) *connTestHelper {
	conn, err := OpenConn("localhost:9042", nil, ConnConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return &connTestHelper{t: t, conn: conn}
}

func (h *connTestHelper) exec(cql string) {
	h.t.Helper()
	s := Statement{
		Content:     cql,
		Consistency: frame.ONE,
	}
	if _, err := h.conn.Query(s, nil); err != nil {
		h.t.Fatal(err)
	}
}

func TestConnMassiveQueryIntegration(t *testing.T) {
	h := newConnTestHelper(t)
	defer h.conn.close()

	h.exec("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	h.exec("CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))")
	h.exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')")
	h.exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')")

	query := Statement{Content: "SELECT * FROM mykeyspace.users", Consistency: frame.ONE}
	expected := []frame.Row{
		{
			frame.Bytes{0x0, 0x0, 0x0, 0x1},
			frame.Bytes{'r', 'i', 'c', 'k'},
			frame.Bytes{'s', 'a', 'n', 'c', 'h', 'e', 'z'},
		},
		{
			frame.Bytes{0x0, 0x0, 0x0, 0x4},
			frame.Bytes{'r', 'u', 's', 't'},
			frame.Bytes{'c', 'o', 'h', 'l', 'e'},
		},
	}

	const n = 1500

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			res, err := h.conn.Query(query, nil)
			if err != nil {
				t.Fatal(err)
			}

			if len(res.Rows) != 2 {
				t.Fatal("invalid number of rows")
			}

			for j, row := range res.Rows {
				if diff := cmp.Diff(expected[j], row); diff != "" {
					t.Fatal(diff)
				}
			}
		}()
	}

	wg.Wait()
}

var benchmarkConnQueryResult QueryResult

func BenchmarkConnQueryIntegration(b *testing.B) {
	h := newConnTestHelper(b)
	h.exec("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	h.exec("CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))")
	h.exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')")
	h.exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')")

	query := Statement{Content: "SELECT * FROM mykeyspace.users", Consistency: frame.ONE}

	b.ResetTimer()

	var (
		r   QueryResult
		err error
	)
	for n := 0; n < b.N; n++ {
		r, err = h.conn.Query(query, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
	benchmarkConnQueryResult = r
}

func TestCloseHangingIntegration(t *testing.T) {
	h := newConnTestHelper(t)
	h.exec("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	h.exec("CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))")
	h.exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')")
	h.exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')")

	query := Statement{Content: "SELECT * FROM mykeyspace.users", Consistency: frame.ONE}

	const n = 10000
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			res, err := h.conn.Query(query, nil)
			if len(res.Rows) != 2 && err == nil {
				t.Fatalf("invalid number of rows")
			}
			// Shut the connection down in the middle of querying
			if id == n/2 {
				h.conn.close()
			}
		}(i)

	}

	wg.Wait()

	// After closing all queries should return an error.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h.conn.Query(query, nil)
			if err == nil {
				t.Fatalf("connection should be closed!")
			}
		}()

	}

	wg.Wait()
}
