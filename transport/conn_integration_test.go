//go:build integration

package transport

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mmatczuk/scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

var dummyConnConfig = ConnConfig{
	Username: "cassandra",
	Password: "cassandra",
	Timeout:  250 * time.Millisecond,
}

func TestOpenShardConnIntegration(t *testing.T) {
	si := ShardInfo{
		Shard:    1,
		NrShards: 2, // Scylla node from docker-compose has only 2 shards
	}

	c, err := OpenShardConn(TestHost+":19042", si, dummyConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	if c.shard != si.Shard {
		t.Fatal("wrong shard", c)
	}
	c.Close()
}

type connTestHelper struct {
	t    testing.TB
	conn *Conn
}

func newConnTestHelper(t testing.TB) *connTestHelper {
	conn, err := OpenConn(TestHost+":9042", nil, dummyConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	return &connTestHelper{t: t, conn: conn}
}

func (h *connTestHelper) applyFixture() {
	h.exec("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	h.exec("CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))")
	h.exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')")
	h.exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')")
}

func (h *connTestHelper) setupMassiveUsersTable() {
	h.exec("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	h.exec("CREATE TABLE IF NOT EXISTS mykeyspace.massive_users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))")
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

func (h *connTestHelper) close() {
	h.conn.Close()
}

func cqlText(s string) frame.CqlValue {
	v, _ := frame.CqlFromText(s)
	return v
}

func TestConnMassiveQueryIntegration(t *testing.T) {
	h := newConnTestHelper(t)
	h.setupMassiveUsersTable()
	defer h.close()
	defer h.exec("DROP KEYSPACE mykeyspace")

	const n = maxStreamID

	makeInsert := func(id int) Statement {
		return Statement{
			Content:     "INSERT INTO mykeyspace.massive_users(user_id, fname, lname) VALUES (" + strconv.Itoa(id) + ", 'rick', 'sanchez')",
			Consistency: frame.ONE,
		}
	}

	makeQuery := func(id int) Statement {
		return Statement{Content: "SELECT * FROM mykeyspace.massive_users WHERE user_id =" + strconv.Itoa(id), Consistency: frame.ONE}
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			if _, err := h.conn.Query(makeInsert(id), nil); err != nil {
				t.Fatal(err)
			}

			res, err := h.conn.Query(makeQuery(id), nil)
			if err != nil {
				t.Fatal(err)
			}

			if len(res.Rows) != 1 {
				t.Fatal("invalid number of rows")
			}

			for _, row := range res.Rows {
				if diff := cmp.Diff(frame.Row{frame.CqlFromInt32(int32(id)), cqlText("rick"), cqlText("sanchez")}, row); diff != "" {
					t.Fatal(diff)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestCloseHangingIntegration(t *testing.T) {
	h := newConnTestHelper(t)
	h.applyFixture()
	defer h.close()

	query := Statement{Content: "SELECT * FROM mykeyspace.users", Consistency: frame.ONE}

	const n = 1000
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
				h.exec("DROP KEYSPACE mykeyspace")
				h.conn.Close()
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
