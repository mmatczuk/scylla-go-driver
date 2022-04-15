module github.com/mmatczuk/scylla-go-driver

go 1.18

require (
	github.com/google/btree v1.0.1
	github.com/google/go-cmp v0.5.6
	go.uber.org/atomic v1.9.0
	go.uber.org/goleak v1.1.12
)

require github.com/pkg/profile v1.6.0

replace github.com/google/btree => github.com/Michal-Leszczynski/btree v1.0.2-0.20220412174850-ff8ccb34568a
