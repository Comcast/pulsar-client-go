// Copyright 2018 Comcast Cable Communications Management, LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pulsar

import (
	"math/rand"
	"sync"
	"time"
)

// ################
// helper functions
// ################

var (
	randStringChars = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	randStringMu    = new(sync.Mutex) //protects randStringRand, which isn't threadsafe
	randStringRand  = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func randString(n int) string {
	b := make([]rune, n)
	l := len(randStringChars)
	randStringMu.Lock()
	for i := range b {
		b[i] = randStringChars[randStringRand.Intn(l)]
	}
	randStringMu.Unlock()
	return string(b)
}
