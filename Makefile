COMPOSE := docker-compose

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go test ./...

.PHONY: test-no-cache
test-no-cache:
	go test -count=1 ./...

integration-test: RUN=Integration
integration-test:
	go test -v -tags integration -run $(RUN) -race ./transport $(ARGS)

integration-bench: RUN=Integration
integration-bench:
	go test -v -tags integration -run XXX -bench=$(RUN) -benchmem -benchtime=5s -cpuprofile cpu.out ./transport $(ARGS)
	go tool pprof -http :8080 cpu.out

.PHONY: scylla-up
scylla-up:
	@$(COMPOSE) up -d

.PHONY: scylla-down
scylla-down:
	@$(COMPOSE) down --volumes --remove-orphans

.PHONY: scylla-logs
scylla-logs:
	@$(COMPOSE) exec node tail -f /var/log/syslog

.PHONY: scylla-bash
scylla-bash:
	@$(COMPOSE) exec node bash

.PHONY: scylla-cqlsh
scylla-cqlsh:
	@$(COMPOSE) exec node cqlsh
