package main

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mmatczuk/scylla-go-driver"
	"github.com/pkg/profile"
)

const insertStmt = "INSERT INTO benchks.benchtab (pk, v1, v2) VALUES(?, ?, ?)"
const selectStmt = "SELECT v1, v2 FROM benchks.benchtab WHERE pk = ?"

func main() {
	p := profile.Start(profile.CPUProfile, profile.ProfilePath("pprof/"), profile.NoShutdownHook)
	defer p.Stop()

	config := readConfig()
	if !config.dontPrepare {
		initSession, err := scylla.NewSession(scylla.DefaultSessionConfig("", config.nodeAddresses...))
		if err != nil {
			log.Fatal(err)
		}
		initKeyspaceAndTable(initSession)
	}

	session, err := scylla.NewSession(scylla.DefaultSessionConfig("benchks", config.nodeAddresses...))
	if err != nil {
		log.Fatal(err)
	}
	if config.workload == Selects && !config.dontPrepare {
		initSelectsBenchmark(session, config)
	}

	log.Println("Starting the benchmark")
	startTime := time.Now()

	insertQ, err := session.Prepare(insertStmt)
	if err != nil {
		log.Fatal(err)
	}
	selectQ, err := session.Prepare(selectStmt)
	if err != nil {
		log.Fatal(err)
	}

	if config.workload == Inserts || config.workload == Mixed {
		for pk := int64(0); pk < config.tasks; pk++ {
			insertQ.BindInt64(0, pk)
			insertQ.BindInt64(1, 2*pk)
			insertQ.BindInt64(2, 3*pk)
			insertQ.AsyncExec()
		}
		for pk := int64(0); pk < config.tasks; pk++ {
			if _, err := insertQ.Fetch(); err != nil {
				log.Fatal(err)
			}
		}
	}
	if config.workload == Selects || config.workload == Mixed {
		for pk := int64(0); pk < config.tasks; pk++ {
			selectQ.BindInt64(0, pk)
			selectQ.AsyncExec()
		}
		for pk := int64(0); pk < config.tasks; pk++ {
			res, err := selectQ.Fetch()
			if err != nil {
				log.Fatal(err)
			}

			if len(res.Rows) != 1 {
				log.Fatalf("expected 1 row, got %d", len(res.Rows))
			}

			v1, err := res.Rows[0][0].AsInt64()
			if err != nil {
				log.Fatal(err)
			}
			v2, err := res.Rows[0][1].AsInt64()
			if err != nil {
				log.Fatal(err)
			}
			if v1 != 2*pk || v2 != 3*pk {
				log.Fatalf("expected (%d, %d), got (%d, %d)", 2*pk, 3*pk, v1, v2)
			}
		}
	}

	benchTime := time.Now().Sub(startTime)
	log.Printf("Finished\nBenchmark time: %d ms\n", benchTime.Milliseconds())
}

func initKeyspaceAndTable(session *scylla.Session) {
	q := session.Query("DROP KEYSPACE IF EXISTS benchks")
	if _, err := q.Exec(); err != nil {
		log.Fatal(err)
	}

	q = session.Query("CREATE KEYSPACE IF NOT EXISTS benchks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}")
	if _, err := q.Exec(); err != nil {
		log.Fatal(err)
	}

	q = session.Query("CREATE TABLE IF NOT EXISTS benchks.benchtab (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)")
	if _, err := q.Exec(); err != nil {
		log.Fatal(err)
	}
}

func initSelectsBenchmark(session *scylla.Session, config Config) {
	log.Println("inserting values...")

	var wg sync.WaitGroup
	nextBatchStart := int64(0)

	for i := int64(0); i < max(1024, config.concurrency); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			insertQ, err := session.Prepare(insertStmt)
			if err != nil {
				log.Fatal(err)
			}

			for {
				curBatchStart := atomic.AddInt64(&nextBatchStart, config.batchSize)
				if curBatchStart >= config.tasks {
					// no more work to do
					break
				}

				curBatchEnd := min(curBatchStart+config.batchSize, config.tasks)

				for pk := curBatchStart; pk < curBatchEnd; pk++ {
					insertQ.BindInt64(0, pk)
					insertQ.BindInt64(1, 2*pk)
					insertQ.BindInt64(2, 3*pk)
					if _, err := insertQ.Exec(); err != nil {
						log.Fatal(err)
					}
				}
			}
		}()
	}

	wg.Wait()
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}
