package transport

import (
	"log"
	"time"
)

const (
	MovingAverageStrategy = 0x01
	FixedStrategy         = 0x02

	movingAverageWindowSize = 16
)

// CoalescingStrategy defines how long to pile-up frames before coalescing.
type CoalescingStrategy interface {
	// Waits for a period of time to pile-up frames.
	wait()

	// Notifies the strategy that a request was just sent.
	notify()
}

func makeCoalescingStrategy(strategy int, maxWaitTime time.Duration) CoalescingStrategy {
	switch strategy {
	case MovingAverageStrategy:
		return &movingAverageStrategy{maxWaitTime: maxWaitTime, last: time.Now()}
	case FixedStrategy:
		return fixedStrategy{waitTime: maxWaitTime}
	default:
		log.Fatalf("Unknown strategy %v", strategy)
		return nil
	}
}

// movingAverageStrategy maintains a moving average of recent gaps, waits for the time of doubled average.
type movingAverageStrategy struct {
	gaps  [movingAverageWindowSize]time.Duration
	pos   int
	total time.Duration

	last        time.Time
	ready       bool
	maxWaitTime time.Duration
}

func (s *movingAverageStrategy) wait() {
	if s.ready {
		waitTime := 2 * s.total / movingAverageWindowSize
		if waitTime > s.maxWaitTime {
			waitTime = s.maxWaitTime
		}

		time.Sleep(waitTime)
	}
}

func (s *movingAverageStrategy) notify() {
	gap := time.Since(s.last)
	s.last = time.Now()
	s.total += gap - s.gaps[s.pos]
	s.gaps[s.pos] = gap

	s.pos++
	if s.pos == 16 {
		s.ready = true
		s.pos = 0
	}
}

// fixedStrategy piles up requests for a fixed period of time.
type fixedStrategy struct {
	waitTime time.Duration
}

func (s fixedStrategy) wait() {
	time.Sleep(s.waitTime)
}

func (s fixedStrategy) notify() {}
