//go:build integration

package scylla

import (
	"context"
	"testing"
)

func BenchmarkSessionQueryIntegration(b *testing.B) {
	session := newTestSession(b)

	initStmts := []string{
		"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS mykeyspace.triples (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
		"TRUNCATE TABLE mykeyspace.triples",
	}

	for _, stmt := range initStmts {
		q := session.Query(stmt)
		if _, err := q.Exec(context.Background()); err != nil {
			b.Fatal(err)
		}
	}

	insertQuery, err := session.Prepare(context.Background(), insertStmt)
	if err != nil {
		b.Fatal(err)
	}

	selectQuery, err := session.Prepare(context.Background(), selectStmt)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := int64(0); i < int64(b.N); i++ {
		insertQuery.BindInt64(0, i).BindInt64(1, 2*i).BindInt64(2, 3*i)
		_, err := insertQuery.Exec(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	}

	for i := int64(0); i < int64(b.N); i++ {
		selectQuery.BindInt64(0, i)
		res, err := selectQuery.Exec(context.Background())
		if err != nil {
			b.Fatal(err)
		}

		if len(res.Rows) != 1 {
			b.Fatalf("expected 1 row, got %d", len(res.Rows))
		}

		v1, err := res.Rows[0][0].AsInt64()
		if err != nil {
			b.Fatal(err)
		}
		v2, err := res.Rows[0][1].AsInt64()
		if err != nil {
			b.Fatal(err)
		}
		if v1 != 2*i || v2 != 3*i {
			b.Fatalf("expected (%d, %d), got (%d, %d)", 2*i, 3*i, v1, v2)
		}
	}
}

func BenchmarkSessionAsyncQueryIntegration(b *testing.B) {
	session := newTestSession(b)

	initStmts := []string{
		"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
		"CREATE TABLE IF NOT EXISTS mykeyspace.triples (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
		"TRUNCATE TABLE mykeyspace.triples",
	}

	for _, stmt := range initStmts {
		q := session.Query(stmt)
		if _, err := q.Exec(context.Background()); err != nil {
			b.Fatal(err)
		}
	}

	insertQuery, err := session.Prepare(context.Background(), insertStmt)
	if err != nil {
		b.Fatal(err)
	}

	selectQuery, err := session.Prepare(context.Background(), selectStmt)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := int64(0); i < int64(b.N); i++ {
		insertQuery.BindInt64(0, i).BindInt64(1, 2*i).BindInt64(2, 3*i)
		insertQuery.AsyncExec(context.Background())
	}

	for i := int64(0); i < int64(b.N); i++ {
		if _, err = insertQuery.Fetch(); err != nil {
			b.Fatal(err)
		}
	}

	for i := int64(0); i < int64(b.N); i++ {
		selectQuery.BindInt64(0, i)
		selectQuery.AsyncExec(context.Background())
	}

	for i := int64(0); i < int64(b.N); i++ {
		res, err := selectQuery.Fetch()
		if err != nil {
			b.Fatal(err)
		}

		if len(res.Rows) != 1 {
			b.Fatalf("expected 1 row, got %d", len(res.Rows))
		}

		v1, err := res.Rows[0][0].AsInt64()
		if err != nil {
			b.Fatal(err)
		}
		v2, err := res.Rows[0][1].AsInt64()
		if err != nil {
			b.Fatal(err)
		}
		if v1 != 2*i || v2 != 3*i {
			b.Fatalf("expected (%d, %d), got (%d, %d)", 2*i, 3*i, v1, v2)
		}
	}
}
