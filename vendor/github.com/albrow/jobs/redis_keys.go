// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

// keys stores any constant redis keys. By storing them all here,
// we avoid using string literals which are prone to typos.
var Keys = struct {
	// jobsTimeIndex is the key for a sorted set which keeps all outstanding
	// jobs sorted by their time field.
	JobsTimeIndex string
	// jobsTemp is the key for a temporary set which is created and then destroyed
	// during the process of getting the next jobs in the queue.
	JobsTemp string
	// activePools is the key for a set which holds the pool ids for all active
	// pools.
	ActivePools string
}{
	JobsTimeIndex: "jobs:time",
	JobsTemp:      "jobs:temp",
	ActivePools:   "pools:active",
}
