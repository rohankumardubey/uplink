// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package scheduler

import (
	"time"
)

type durationBuffer struct {
	slots   []time.Duration
	index   uint
	initial time.Duration
}

func (d *durationBuffer) init(cap int, initial time.Duration) {
	d.slots = make([]time.Duration, 0, cap)
	d.index = 0
	d.initial = initial
}

func (d *durationBuffer) Record(x time.Duration) {
	if len(d.slots) < cap(d.slots) {
		d.slots = append(d.slots, x)
	} else if len(d.slots) > 0 {
		d.slots[d.index%uint(len(d.slots))] = x
	}
	d.index++
}

func (d *durationBuffer) Minimum() time.Duration {
	if len(d.slots) == 0 {
		return d.initial
	}
	min := d.slots[0]
	for _, v := range d.slots[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

func (d *durationBuffer) Average() time.Duration {
	if len(d.slots) == 0 {
		return d.initial
	}
	sum := d.slots[0]
	for _, v := range d.slots[1:] {
		sum += v
	}
	return time.Duration(float64(sum) / float64(len(d.slots)))
}
