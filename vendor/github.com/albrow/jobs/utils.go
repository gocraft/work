// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"github.com/dchest/uniuri"
	"strconv"
	"time"
)

// generateRandomId generates a random string that is more or less
// garunteed to be unique.
func generateRandomId() string {
	timeInt := time.Now().UnixNano()
	timeString := strconv.FormatInt(timeInt, 36)
	randomString := uniuri.NewLen(16)
	return randomString + timeString
}
