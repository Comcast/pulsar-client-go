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
	"io"
)

// Fuzz is an entrypoint for fuzz testing done by
// the `go-fuzz` program. This tests that frame.Decode
// can handle any input given.
//
// See the /fuzz directory for more information.
func Fuzz(data []byte) int {
	var f Frame
	if err := f.Decode(bytes.NewReader(data)); err != nil {
		return 0
	}

	return 1
}

// Fuzz is an entrypoint for fuzz testing done by
// the `go-fuzz` program. This function performs additional
// validation by ensuring that the re-encoded frame is equal
// to the original input data.
//
// See the /fuzz directory for more information.
func FuzzReEncode(data []byte) int {
	var b bytes.Buffer
	// copy to b the portion of data that Decode consumes
	input := io.TeeReader(bytes.NewReader(data), &b)

	var f Frame
	if err := f.Decode(input); err != nil {
		return 0
	}

	consumed := make([]byte, b.Len())
	copy(consumed, b.Bytes())
	b.Reset()

	// Ensure that the re-encoded frame matches the
	// original input byte-for-byte (ignoring any additional
	// bytes that Decode did not consume)
	if err := f.Encode(&b); err != nil {
		panic(err)
	}
	reencoded := b.Bytes()

	if !bytes.Equal(consumed, reencoded) {
		panic(fmt.Sprintf(
			"re-encoded %q frame does not match original input:\noriginal:\n%s\nre-encoded:\n%s\n",
			f.BaseCmd.GetType(),
			hex.Dump(consumed),
			hex.Dump(reencoded),
		))
	}

	return 1
}
