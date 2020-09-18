// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package scheduler

import (
	"context"
	"sync"
	"time"
)

// The Scheduler design is intricate. It manages Resources and Handles.
// The Scheduler allows you to Join and gives you back a Handle, and
// that Handle can be used to Get Resources from the Scheduler. The reason
// for this two level split is so that earlier Joined Handles are given
// priority over later Joined Handles when not enough Resources are available.
// The Scheduler maintains minimum number of Resources allowed to exist at
// once by not blocking Get if there is less than the minimum in existence.
// It also maintains a maximum number of Resources allowed to exist at once
// by blocking forever until the number of existing Resources is low enough.
// It adds Resources available to be acquired above the minimum periodically
// based on some rate. That rate is determined by looking at the duration
// that Resources are alive until they are Done. Specifically, it takes the
// minimum of a ring buffer of those durations. Additionally, the Scheduler
// allows canceling Getting of a Resource by canceling a context. Enjoy!
//
// As far as the implementation goes, this Scheduler has some nice properties:
// - No goroutines are started or maintained or need to be closed
// - No allocations in the common case of not needing to sleep for more
//   resources

type Scheduler struct {
	now  func() time.Time
	opts Options

	wait    chan struct{} // closed and bumped when a resource is done
	waiters int

	mu      sync.Mutex
	last    time.Time // last time a resource was included from timers
	dbuf    durationBuffer
	handles []*handle

	held int // number of held resources
	free int // number of free resources
}

type Options struct {
	DurationBuffer    int           // number of entries to include in duration buffer
	InitialDuration   time.Duration // time to add resources until we learn some durations
	InitialConcurrent int           // initial number of concurrent resources
	MaximumConcurrent int           // number of maximum concurrent resources
}

func New(opts Options) *Scheduler {
	sched := &Scheduler{
		now:  time.Now,
		opts: opts,
		wait: make(chan struct{}, 1),
		free: opts.InitialConcurrent,
	}
	sched.dbuf.init(opts.DurationBuffer, opts.InitialDuration)

	return sched
}

func (s *Scheduler) resourceGet(ctx context.Context, h *handle) bool {
	var next *time.Timer
	defer func() {
		if next != nil {
			next.Stop()
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	// lazily initialize s.last so that tests can overwrite the now function
	if s.last.IsZero() {
		s.last = s.now()
	}

main:
	for {
		if ctx.Err() != nil {
			return false
		}

		// first, check to see if we have slots available, or if enough time
		// has elapsed for there to be a new slot, or calculate how long to
		// delay until there will be a free slot available.
		var delay time.Duration

		if rem := s.opts.MaximumConcurrent - s.held; s.free == 0 && rem > 0 {
			min := s.dbuf.Minimum()
			// if 1/min.Seconds() < float64(s.opts.InitialConcurrent/2) {
			// 	newmin := time.Duration(float64(time.Second) * 1 / float64(s.opts.InitialConcurrent/2))
			// 	elog.Printf("min too large:%v initial:%d newmin:%v\n", min, s.opts.InitialConcurrent, newmin)
			// 	min = newmin
			// }

			// if we have a zero minimum time, then assume we can add as many
			// resources as possible, effectively adding them at an infinite
			// rate.
			if min == 0 {
				s.free += rem
			} else {
				// otherwise calculate how many to add based on the current duration
				now := s.now()
				dur := now.Sub(s.last)
				add := int(dur / min)

				// if it's over the amount we have remaining, cap it.
				if add > rem {
					add = rem
				}

				// if we have any to add, then add them and update the timestamp
				// for when we last added any values. otherwise, set up a delay
				// for how long until we can add some free resources.
				if add > 0 {
					s.free += add
					s.last = s.now()
				} else {
					delay = min - dur
				}
			}
		}

		// if we have any free slots, try the handles in order. earlier
		// handles should get priority over later handles. it is guaranteed
		// to give the free value to a handle.
		if s.free > 0 {
			for _, ch := range s.handles {
				ch.mu.Lock()

				// if the handle is not waiting, check the next one.
				if !ch.wait {
					ch.mu.Unlock()

					// if this is our handle that is no longer waiting, then
					// clean up the signal channel and exit.
					if ch == h {
						<-h.sig
						return true
					}

					continue
				}

				// flag that it no longer needs to wait and if the handle is
				// not ourselves, send a value into the signal in case they are
				// blocked in the slow select path.
				ch.wait = false
				if ch.sig != nil && ch != h {
					ch.sig <- struct{}{}
				}

				ch.mu.Unlock()

				// we acquired a resource so update that state.
				s.free--
				s.held++

				// if we notified ourselves, then we're done. no need to clean
				// up the signal channel because we did not send in this case.
				if ch == h {
					return true
				}

				continue main
			}

			panic("unreachable")
		}

		// we're about to go into waiting for either the timer, the context, a
		// resource free, or another resource to inform us we're done. if we're
		// lucky, another resource has already informed us and set our waiting
		// flag to false. if so, we've acquired and there's no need to allocate
		// a channel or anything.
		h.mu.Lock()

		// in this case, someone else has notified us that we're done.
		if !h.wait {
			h.mu.Unlock()

			// read the value from the signal channel that must have been sent.
			<-h.sig
			return true
		}

		// otherwise, we have to wait to be notified on our channel.
		sig := h.sig
		gotSig := false

		h.mu.Unlock()

		// set up the timer channel if we have a sleep time specified.
		var nextCh <-chan time.Time
		if delay > 0 {
			if next != nil {
				next.Stop()
			}
			next = time.NewTimer(delay)
			nextCh = next.C
		}

		s.waiters++
		s.mu.Unlock()

		// elog.Printf("handle:%p waiting up to %v for token\n", h, delay)

		select {
		case <-ctx.Done():
		case <-nextCh:
		case <-s.wait:
		case <-sig:
			gotSig = true
		}

		s.mu.Lock()
		s.waiters--

		if gotSig {
			return true
		}
	}
}

func (s *Scheduler) getWaiters() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.waiters
}

func (s *Scheduler) resourceDone(acquired time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.held--
	s.dbuf.Record(s.now().Sub(acquired))

	// elog.Printf("- resourceDone dur:%v mdir:%v held:%v avg:%v\n", s.now().Sub(acquired), s.dbuf.Minimum(), s.held, s.dbuf.Average())

	// if someone is waiting for a resource, let them know that one is
	// no longer being held. we can wake any waiter and be fine because
	// they all do the work of trying to give the free resource to the
	// best handle.
	select {
	case s.wait <- struct{}{}:
	default:
	}
}

func (s *Scheduler) handleDone(h *handle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, ch := range s.handles {
		if ch == h {
			s.handles = append(s.handles[:i], s.handles[i+1:]...)
			return
		}
	}
}

func (s *Scheduler) Join() Handle {
	s.mu.Lock()
	defer s.mu.Unlock()

	h := &handle{
		sched: s,
		sig:   make(chan struct{}, 1),
	}
	s.handles = append(s.handles, h)

	return h
}

//
// Handle
//

type Handle interface {
	Get(context.Context) (Resource, bool)
	Done()
}

type handle struct {
	mu    sync.Mutex
	wg    sync.WaitGroup
	sched *Scheduler
	sig   chan struct{}
	wait  bool
}

func (h *handle) Done() {
	// transition to the Done state by clearing the sched pointer.
	h.mu.Lock()
	sched := h.sched
	if sched == nil {
		h.mu.Unlock()
		return
	}
	h.sched = nil
	h.mu.Unlock()

	// wait for acquired resources and inform the scheduler.
	h.wg.Wait()
	sched.handleDone(h)

	// elog.Println("+ Segment Done", len(sched.handles))
}

func (h *handle) Get(ctx context.Context) (Resource, bool) {
	h.mu.Lock()
	if h.sched == nil {
		h.mu.Unlock()
		return nil, false
	}

	// flag the handle as desiring a resource
	h.wait = true

	h.mu.Unlock()

	// pump the scheduler to dispatch resources
	ok := h.sched.resourceGet(ctx, h)

	h.mu.Lock()
	defer h.mu.Unlock()

	h.wait = false

	// if ok is still false, then we didn't get a resource for
	// whatever reason.
	if !ok {
		return nil, false
	}

	h.wg.Add(1)
	return &resource{
		handle:   h,
		sched:    h.sched,
		acquired: h.sched.now(),
	}, true
}

//
// Resource
//

type Resource interface {
	Done()
}

type resource struct {
	handle   *handle
	sched    *Scheduler
	acquired time.Time
}

func (r *resource) Done() {
	r.sched.resourceDone(r.acquired)
	r.handle.wg.Done()
}
