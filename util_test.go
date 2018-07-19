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
	"bufio"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// ################
// helper functions
// ################

// hexUndump is the inverse of hex.Dump. It massages the output of hex.Dump
// into a hex string, and then runs hex.DecodeString over it.
// Example line:
// 00000000  00 00 00 19 00 00 00 15  08 03 1a 11 0a 0d 50 75  |..............Pu|
//        8<------------------------------------------------>58
func hexUndump(h string) []byte {
	var out string

	hexReplacer := strings.NewReplacer(" ", "")
	s := bufio.NewScanner(strings.NewReader(strings.TrimSpace(h)))
	for s.Scan() {
		line := s.Text()
		if len(line) < 58 {
			panic(fmt.Sprintf("invalid hex line: %q", line))
		}
		out += hexReplacer.Replace(line[8:58])
	}
	if err := s.Err(); err != nil {
		panic(err)
	}

	b, err := hex.DecodeString(out)
	if err != nil {
		panic(err)
	}
	return b
}

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
