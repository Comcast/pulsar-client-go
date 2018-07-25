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

package frame

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"strings"
)

// ################
// helper functions
// ################

// hexUndump is the inverse of hex.Dump. It massages the output of hex.Dump
// into a hex string, and then runs hex.DecodeString over it.
// Example line:
// 00000000  00 00 00 19 00 00 00 15  08 03 1a 11 0a 0d 50 75  |..............Pu|
//        8<------------------------------------------------>58
// It is also capable of decoding BSD's hexdump output.

func hexUndump(h string) []byte {
	var out string

	s := bufio.NewScanner(strings.NewReader(strings.TrimSpace(h)))
	for s.Scan() {
		line := s.Text()
		if len(line) == 0 {
			panic(fmt.Sprintf("invalid hex line: %q", line))
		}
		// first starting delimiter, which is the first space which separates the
		// offset from the hex encoded data
		firstSpace := strings.IndexRune(line, ' ')
		if firstSpace == -1 {
			panic(fmt.Sprintf("invalid hex line: %q", line))
		}
		line = line[firstSpace:]
		// possible ending delimiter, which is the start of ASCII representation
		// of the data
		if ascii := strings.IndexRune(line, '|'); ascii > 0 {
			line = line[:ascii]
		}

		// remove spaces between hex numbers
		line = strings.Replace(line, " ", "", -1)

		end := 32
		if len(line) < end {
			end = len(line)
		}
		out += line[:end]
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
