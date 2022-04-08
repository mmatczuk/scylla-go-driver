export OS    := $(shell uname)
export GOBIN := $(PWD)/bin
export PATH  := $(GOBIN):$(PATH)

.PHONY: install-dependencies
install-dependencies:
	@rm -Rf bin
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: check
check:
	@$(GOBIN)/golangci-lint run ./...

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go test ./...

.PHONY: test-no-cache
test-no-cache:
	go test -count=1 ./...

COMPOSE := docker-compose

.PHONY: integration-test
integration-test:
	@$(MAKE) pkg-integration-test PKG=./transport
	@$(MAKE) pkg-integration-test PKG=./

# Prevent invoking make with a package specific test without a constraining a package.
ifneq "$(filter pkg-%,$(MAKECMDGOALS))" ""
ifeq "$(PKG)" ""
$(error Please specify package name with PKG e.g. PKG=./transport)
endif
endif

.PHONY: pkg-integration-test
pkg-integration-test: RUN=Integration
pkg-integration-test:
ifeq ($(OS),Linux)
	go test -v -tags integration -run $(RUN) -short $(PKG) $(ARGS)
else ifeq ($(OS),Darwin)
	@CGO_ENABLED=0 GOOS=linux go test -v -tags integration -c -o ./integration-test.dev $(PKG)
	@docker run --name "integration-test" \
		--network scylla_go_driver_public \
		-v "$(PWD)/integration-test.dev:/usr/bin/integration-test:ro" \
		-it --read-only --rm ubuntu integration-test -test.v -test.run $(RUN) -test.short $(ARGS)
else
	$(error Unsupported OS $(OS))
endif

.PHONY: run-benchtab
run-benchtab:
ifeq ($(OS),Linux)
	# TODO add runner remember taskset
else ifeq ($(OS),Darwin)
	@CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-extldflags '-static'" -o ./benchtab.dev ./experiments/cmd/benchtab
	@docker run --name "benchtab" \
		--network scylla_go_driver_public \
		-v "$(PWD)/benchtab.dev:/usr/bin/benchtab:ro" \
		-v "$(PWD)/experiments/pprof:/pprof" \
		-it --read-only --rm --cpuset-cpus 2,3 ubuntu benchtab -nodes "192.168.100.100:9042"
else
	$(error Unsupported OS $(OS))
endif

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

