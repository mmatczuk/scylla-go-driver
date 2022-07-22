//go:build integration

package scylla

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"testing"

	"go.uber.org/goleak"
)

const TestHost = "192.168.100.100"

var testingSessionConfig = DefaultSessionConfig("mykeyspace", TestHost)

func initKeyspace(t testing.TB) {
	t.Helper()

	cfg := testingSessionConfig
	cfg.Keyspace = ""
	s, err := NewSession(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	q := s.Query("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	if _, err = q.Exec(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.Close()
}

func newTestSession(t testing.TB) *Session {
	t.Helper()

	initKeyspace(t)
	s, err := NewSession(context.Background(), testingSessionConfig)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSessionIntegration(t *testing.T) {
	defer goleak.VerifyNone(t)
	session := newTestSession(t)
	defer session.Close()

	stmts := []string{
		"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))",
		"INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')",
		"INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')",
	}

	for _, stmt := range stmts {
		q := session.Query(stmt)
		if _, err := q.Exec(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	q := session.Query("SELECT * FROM mykeyspace.users")

	res, err := q.Exec(context.Background())
	if err != nil {
		t.Fatalf("couldn't query: %v", err)
	}

	for _, row := range res.Rows {
		pk, err := row[0].AsInt32()
		if err != nil {
			t.Fatal(err)
		}
		name, err := row[1].AsText()
		if err != nil {
			t.Fatal(err)
		}
		surname, err := row[2].AsText()
		if err != nil {
			t.Fatal(err)
		}

		t.Log(pk, name, surname)
	}
}

const (
	insertStmt = "INSERT INTO mykeyspace.triples (pk, v1, v2) VALUES(?, ?, ?)"
	selectStmt = "SELECT v1, v2 FROM mykeyspace.triples WHERE pk = ?"
)

func TestSessionPrepareIntegration(t *testing.T) { // nolint:paralleltest // Integration tests are not run in parallel!
	defer goleak.VerifyNone(t)
	session := newTestSession(t)
	defer session.Close()

	initStmts := []string{
		"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS mykeyspace.triples (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
		"TRUNCATE mykeyspace.triples",
	}

	for _, stmt := range initStmts {
		q := session.Query(stmt)
		if _, err := q.Exec(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	insertQuery, err := session.Prepare(context.Background(), insertStmt)
	if err != nil {
		t.Fatal(err)
	}

	selectQuery, err := session.Prepare(context.Background(), selectStmt)
	if err != nil {
		t.Fatal(err)
	}

	for i := int64(0); i < 100; i++ {
		insertQuery.BindInt64(0, i).BindInt64(1, 2*i).BindInt64(2, 3*i)
		res, err := insertQuery.Exec(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		selectQuery.BindInt64(0, i)
		res, err = selectQuery.Exec(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		if len(res.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(res.Rows))
		}

		v1, err := res.Rows[0][0].AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		v2, err := res.Rows[0][1].AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		if v1 != 2*i || v2 != 3*i {
			t.Fatalf("expected (%d, %d), got (%d, %d)", 2*i, 3*i, v1, v2)
		}
	}
}

func TestSessionIterIntegration(t *testing.T) { // nolint:paralleltest // Integration tests are not run in parallel!
	defer goleak.VerifyNone(t)
	session := newTestSession(t)
	defer session.Close()

	initStmts := []string{
		"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS mykeyspace.triples (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
		"TRUNCATE TABLE mykeyspace.triples",
	}

	for _, stmt := range initStmts {
		q := session.Query(stmt)
		if _, err := q.Exec(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	insertQuery, err := session.Prepare(context.Background(), insertStmt)
	if err != nil {
		t.Fatal(err)
	}

	N := 1000
	for i := int64(0); i < int64(N); i++ {
		insertQuery.BindInt64(0, i).BindInt64(1, 2*i).BindInt64(2, 3*i)

		if _, err := insertQuery.Exec(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	q := session.Query("SELECT * FROM mykeyspace.triples")
	q.SetPageSize(10)

	p, err := session.Prepare(context.Background(), "SELECT * FROM mykeyspace.triples")
	if err != nil {
		t.Fatal(err)
	}
	p.SetPageSize(10)

	iters := [2]Iter{q.Iter(context.Background()), p.Iter(context.Background())}
	for _, it := range iters {
		row, err := it.Next()

		m := make(map[int64]struct{})
		for ; err == nil; row, err = it.Next() {
			x, err := row[0].AsInt64()
			if err != nil {
				t.Fatal(err)
			}
			y, err := row[1].AsInt64()
			if err != nil {
				t.Fatal(err)
			}
			z, err := row[2].AsInt64()
			if err != nil {
				t.Fatal(err)
			}

			if y != 2*x || z != 3*x {
				t.Fatalf("expected (%d, %d, %d), got (%d, %d %d)", x, 2*x, 3*x, x, y, z)
			}
			m[x] = struct{}{}
		}

		if !errors.Is(err, ErrNoMoreRows) {
			t.Fatal(err)
		}

		it.Close()
		_, err = it.Next()
		if err == nil {
			t.Fatal("read on closed iter should fail")
		}

		if len(m) != N {
			t.Fatalf("expected %d different rows, got %d", N, len(m))
		}
	}
}

var (
	caPath   = "testdata/tls/cadb.pem"
	certPath = "testdata/tls/db.crt"
	keyPath  = "testdata/tls/db.key"
)

func newCertPoolFromFile(t *testing.T, path string) *x509.CertPool {
	certPool := x509.NewCertPool()
	pem, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !certPool.AppendCertsFromPEM(pem) {
		t.Fatalf("failed parsing of CA certs")
	}

	return certPool
}

func makeCertificatesFromFiles(t *testing.T, certPath, keyPath string) []tls.Certificate {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}

	return []tls.Certificate{cert}
}

func TestTLSIntegration(t *testing.T) {
	testCases := []struct {
		name      string
		tlsConfig *tls.Config
	}{
		{
			name:      "no tls",
			tlsConfig: nil,
		},
		{
			name: "tls - no client verification",
			tlsConfig: &tls.Config{
				RootCAs:            x509.NewCertPool(),
				InsecureSkipVerify: true,
				Certificates:       makeCertificatesFromFiles(t, certPath, keyPath),
			},
		},
		{
			name: "tls - with client verification",
			tlsConfig: &tls.Config{
				RootCAs:            newCertPoolFromFile(t, caPath),
				InsecureSkipVerify: false,
				ServerName:         "192.168.100.100",
				Certificates:       makeCertificatesFromFiles(t, certPath, keyPath),
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			cfg := testingSessionConfig.Clone()
			cfg.TLSConfig = tc.tlsConfig
			cfg.Keyspace = ""
			cfg.Hosts = []string{"192.168.100.100"}
			if cfg.TLSConfig != nil {
				cfg.DefaultPort = "9142"
			}

			session, err := NewSession(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}

			stmts := []string{
				"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
				"CREATE TABLE IF NOT EXISTS mykeyspace.users (user_id int, fname text, lname text, PRIMARY KEY((user_id)))",
				"INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (1, 'rick', 'sanchez')",
				"INSERT INTO mykeyspace.users(user_id, fname, lname) VALUES (4, 'rust', 'cohle')",
			}

			for _, stmt := range stmts {
				q := session.Query(stmt)
				if _, err := q.Exec(context.Background()); err != nil {
					t.Fatal(err)
				}
			}

			q := session.Query("SELECT COUNT(*) FROM mykeyspace.users")
			if r, err := q.Exec(context.Background()); err != nil {
				t.Fatal(err)
			} else {
				n, err := r.Rows[0][0].AsInt64()
				t.Log(n)
				if err != nil {
					t.Fatal(err)
				}

				if n != 2 {
					t.Fatalf("expected 2, got %d", n)
				}
			}
		})
	}
}
