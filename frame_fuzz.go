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

// +build gofuzz

package pulsar

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

// Fuzz is the entrypoint for fuzz testing done by
// the `go-fuzz` program.
//
// See the /fuzz directory for more information.
func Fuzz(data []byte) int {
	var f Frame
	if err := f.Decode(bytes.NewReader(data)); err != nil {
		return 0
	}

	// Ensure that the re-encoded frame matches the
	// original input byte-for-byte
	var b bytes.Buffer
	if err := f.Encode(&b); err != nil {
		panic(err)
	}
	got := b.Bytes()

	// ignore input data that extends beyond
	// the encoded data
	dataEnd := len(data)
	if l := len(got); dataEnd > l {
		dataEnd = l
	}
	consumed := data[:dataEnd]

	if !bytes.Equal(consumed, got) {
		panic(fmt.Sprintf(
			"re-encoded %q frame does not match original input:\noriginal:\n%s\nre-encoded:\n%s\n",
			f.BaseCmd.GetType(),
			hex.Dump(consumed),
			hex.Dump(got),
		))
	}

	return 1
}
