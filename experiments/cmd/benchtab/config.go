package main

import (
	"flag"
	"log"
	"runtime"
	"strings"
)

type Workload int

const (
	Inserts Workload = iota
	Selects
	Mixed
)

type Config struct {
	nodeAddresses []string
	workload      Workload
	tasks         int64
	workers       int64
	batchSize     int64
	dontPrepare   bool
}

func readConfig() Config {
	config := Config{}

	nodes := flag.String(
		"nodes",
		"",
		"Addresses of database nodes to connect to separated by a comma",
	)

	workload := flag.String(
		"workload",
		"mixed",
		"Type of work to perform (inserts, selects, mixed)",
	)

	flag.Int64Var(
		&config.tasks,
		"tasks",
		1_000_000,
		"Total number of tasks (requests) to perform the during benchmark. In case of mixed workload there will be tasks inserts and tasks selects",
	)

	flag.Int64Var(
		&config.workers,
		"workers",
		int64(runtime.NumCPU()),
		"Maximum number of workers, default nr of CPUs",
	)

	flag.BoolVar(
		&config.dontPrepare,
		"dont-prepare",
		false,
		"Don't create tables and insert into them before the benchmark",
	)

	config.batchSize = config.tasks / config.workers

	flag.Parse()

	for _, node_address := range strings.Split(*nodes, ",") {
		config.nodeAddresses = append(config.nodeAddresses, node_address)
	}

	switch *workload {
	case "inserts":
		config.workload = Inserts
	case "selects":
		config.workload = Selects
	case "mixed":
		config.workload = Mixed
	default:
		log.Fatal("invalid workload type")
	}

	max := func(a, b int64) int64 {
		if a > b {
			return a
		}

		return b
	}

	if config.tasks/config.batchSize < config.workers {
		config.batchSize = max(1, config.tasks/config.workers)
	}

	return config
}
