package ratelimit_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/ratelimit/cancellable"
	"go.uber.org/ratelimit/internal/clock"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/atomic"
)

var background = context.Background()

func ExampleRatelimit() {
	rl := ratelimit.New(100) // per second

	prev := time.Now()
	for i := 0; i < 10; i++ {
		now := rl.Take(background.Done())
		if i > 0 {
			fmt.Println(i, now.Sub(prev))
		}
		prev = now
	}

	// Output:
	// 1 10ms
	// 2 10ms
	// 3 10ms
	// 4 10ms
	// 5 10ms
	// 6 10ms
	// 7 10ms
	// 8 10ms
	// 9 10ms
}

func ExampleRatelimitWithDrop() {
	rl := ratelimit.New(100) // per second

	prev := time.Now()
	for i := 0; i < 10; i++ {
		var now time.Time
		if i%2 == 1 {
			c := make(chan struct{})
			close(c)
			now = rl.Take(c)
		} else {
			now = rl.Take(background.Done())
		}
		if i > 0 {
			fmt.Println(i, now.Sub(prev))
		}
		prev = now
	}
	// Output:
	// 1 0s
	// 2 10ms
	// 3 0s
	// 4 10ms
	// 5 0s
	// 6 10ms
	// 7 0s
	// 8 10ms
	// 9 0s
}

func TestUnlimited(t *testing.T) {
	now := time.Now()
	rl := ratelimit.NewUnlimited()
	for i := 0; i < 1000; i++ {
		rl.Take(background.Done())
	}
	assert.Condition(t, func() bool { return time.Now().Sub(now) < 1*time.Millisecond }, "no artificial delay")
}

func TestRateLimiter(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	clock := clock.NewMock()
	rl := ratelimit.New(100, ratelimit.WithClock(clock), ratelimit.WithoutSlack)

	count := atomic.NewInt32(0)

	// Until we're done...
	done := make(chan struct{})
	defer close(done)

	// Create copious counts concurrently.
	go job(rl, count, done)
	go job(rl, count, done)
	go job(rl, count, done)
	go job(rl, count, done)

	clock.AfterFunc(1*time.Second, func() {
		assert.InDelta(t, 100, count.Load(), 10, "count within rate limit")
	})

	clock.AfterFunc(2*time.Second, func() {
		assert.InDelta(t, 200, count.Load(), 10, "count within rate limit")
	})

	clock.AfterFunc(3*time.Second, func() {
		assert.InDelta(t, 300, count.Load(), 10, "count within rate limit")
		wg.Done()
	})

	clock.Add(4 * time.Second)

	clock.Add(5 * time.Second)
}

func TestDelayedRateLimiter(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	clock := clock.NewMock()
	slow := ratelimit.New(10, ratelimit.WithClock(clock))
	fast := ratelimit.New(100, ratelimit.WithClock(clock))

	count := atomic.NewInt32(0)

	// Until we're done...
	done := make(chan struct{})
	defer close(done)

	// Run a slow job
	go func() {
		for {
			slow.Take(background.Done())
			fast.Take(background.Done())
			count.Inc()
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	// Accumulate slack for 10 seconds,
	clock.AfterFunc(20*time.Second, func() {
		// Then start working.
		go job(fast, count, done)
		go job(fast, count, done)
		go job(fast, count, done)
		go job(fast, count, done)
	})

	clock.AfterFunc(30*time.Second, func() {
		assert.InDelta(t, 1200, count.Load(), 10, "count within rate limit")
		wg.Done()
	})

	clock.Add(40 * time.Second)
}

func TestRateLimiterWithContexts(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	clock := clock.NewMock()
	rps := 100
	rl := ratelimit.New(rps, ratelimit.WithClock(clock), ratelimit.WithoutSlack)

	count := atomic.NewInt32(0)

	// Until we're done...
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			c, cancel := context.WithCancel(context.Background())
			// cancel right before take would happen
			clock.AfterFunc((time.Second / time.Duration(rps+1)), func() {
				cancel()
			})
			rl.Take(c.Done())
			if c.Err() != nil {
				return
			}
			count.Inc()
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	clock.AfterFunc(1*time.Second, func() {
		assert.InDelta(t, 1, count.Load(), 10, "count within rate limit")
		count.Inc()
	})

	clock.AfterFunc(2*time.Second, func() {
		assert.InDelta(t, 2, count.Load(), 10, "count within rate limit")
		count.Inc()
	})

	clock.AfterFunc(3*time.Second, func() {
		assert.InDelta(t, 3, count.Load(), 10, "count within rate limit")
		wg.Done()
	})

	clock.Add(4 * time.Second)

	clock.Add(5 * time.Second)
}

func job(rl ratelimit.Limiter, count *atomic.Int32, done <-chan struct{}) {
	for {
		rl.Take(background.Done())
		count.Inc()
		select {
		case <-done:
			return
		default:
		}
	}
}
