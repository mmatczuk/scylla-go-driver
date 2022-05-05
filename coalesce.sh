BENCH="taskset -c 1-8 go run ./experiments/cmd/benchtab -nodes 192.168.100.100:9042"

# No sleep, max=20
$BENCH -waittime 0 -max-coalesced 20

# No sleep, max=50
$BENCH -waittime 0 -max-coalesced 50

# No sleep, max=100 (default)
$BENCH -waittime 0 -max-coalesced 100

# no sleep, unlimited max
$BENCH -waittime 0 -max-coalesced 1000000

# 200ms sleep, max=20
$BENCH -waittime 200 -max-coalesced 20

# 200ms sleep, max=50
$BENCH -waittime 200 -max-coalesced 50

# 200ms sleep, max=100
$BENCH -waittime 200 -max-coalesced 100

# 200ms sleep, unlimited max
$BENCH -waittime 200 -max-coalesced 1000000

# 1ms sleep, max=20
$BENCH -waittime 1000 -max-coalesced 20

# 1ms sleep, max=50
$BENCH -waittime 1000 -max-coalesced 50

# 1ms sleep, max=100
$BENCH -waittime 1000 -max-coalesced 100

# 1ms sleep, unlimited max
$BENCH -waittime 1000 -max-coalesced 1000000

