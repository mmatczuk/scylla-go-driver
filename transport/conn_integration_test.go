//go:build integration

package transport

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestOpenShardConnIntegration(t *testing.T) {
	si := ShardInfo{
		NrShards: 4,
	}

	for i := uint16(0); i < si.NrShards; i++ {
		si.Shard = i
		c, err := OpenShardConn(context.Background(), TestHost+":19042", si, DefaultConnConfig(""))
		if err != nil {
			t.Fatal(err)
		}
		if c.Shard() != int(si.Shard) {
			t.Fatalf("wrong shard: got %v, wanted %v", c.Shard(), si.Shard)
		}
		c.Close()
	}
}

type connTestHelper struct {
	t    testing.TB
	conn *Conn
}

func newConnTestHelper(t testing.TB) *connTestHelper {
	conn, err := OpenConn(context.Background(), TestHost, nil, DefaultConnConfig(""))
	if err != nil {
		t.Fatal(err)
	}
	return &connTestHelper{t: t, conn: conn}
}

func (h *connTestHelper) applyFixture() {
	h.exec("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	h.exec("CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))")
	h.exec("TRUNCATE TABLE mykeyspace.users")
	h.exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')")
	h.exec("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')")
	if err := h.conn.UseKeyspace("mykeyspace"); err != nil {
		log.Fatalf("use keyspace %v", err)
	}
}

func (h *connTestHelper) setupMassiveUsersTable() {
	h.exec("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	h.exec("CREATE TABLE IF NOT EXISTS mykeyspace.massive_users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))")
	h.exec("TRUNCATE TABLE mykeyspace.massive_users")
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

func (h *connTestHelper) applyCompressionFixture(toSend []byte) {
	h.execCompression("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	h.execCompression("CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))")
	h.execCompression("TRUNCATE TABLE mykeyspace.users")
	h.execCompression(fmt.Sprintf("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, '%s', 'sanchez')", toSend))
	h.execCompression("INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')")
	if err := h.conn.UseKeyspace("mykeyspace"); err != nil {
		log.Fatalf("use keyspace %v", err)
	}
}

func (h *connTestHelper) execCompression(cql string) {
	h.t.Helper()
	s := Statement{
		Content:     cql,
		Consistency: frame.ONE,
		Compression: true,
	}
	if _, err := h.conn.Query(s, nil); err != nil {
		h.t.Fatal(err)
	}
}

func TestCompressionIntegration(t *testing.T) {
	toSend := make([]byte, (1 << 20)) // 1MB
	for i := 0; i < (1 << 20); i++ {
		toSend[i] = 'a' + byte(rand.Intn(26))
	}
	t.Run("snappy", func(t *testing.T) {
		testCompression(t, frame.Snappy, []byte("rick"))
		testCompression(t, frame.Snappy, toSend)
	})
	t.Run("lz4", func(t *testing.T) {
		testCompression(t, frame.Lz4, []byte("rick"))
		testCompression(t, frame.Lz4, toSend)
	})
}

func testCompression(t *testing.T, c frame.Compression, toSend []byte) {
	t.Helper()

	cfg := DefaultConnConfig("")
	cfg.Compression = c
	conn, err := OpenConn(context.Background(), TestHost, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}

	h := &connTestHelper{t: t, conn: conn}
	h.applyCompressionFixture(toSend)

	defer h.close()

	query := Statement{Content: "SELECT * FROM mykeyspace.users", Consistency: frame.ONE, Compression: true}
	expected := []frame.Row{
		{
			frame.CqlFromInt32(1),
			cqlText(string(toSend)),
			cqlText("sanchez"),
		},
		{
			frame.CqlFromInt32(4),
			cqlText("rust"),
			cqlText("cohle"),
		},
	}

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
}
