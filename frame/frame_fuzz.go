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

package frame

import (
	"bytes"
	"encoding/hex"
	"fmt"
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
	var original, reencoded Frame
	b := bytes.NewBuffer(data)

	if err := original.Decode(b); err != nil {
		return 0
	}
	b.Reset()
	if err := original.Encode(b); err != nil {
		panic(err)
	}

	// protobuf is liberal in what it will consume, so
	// we must encoded/decode twice. The first time will
	// remove any extraneous bytes that won't be included
	// in the re-encoded output.

	originalEnc := make([]byte, b.Len())
	copy(originalEnc, b.Bytes())

	if err := reencoded.Decode(b); err != nil {
		panic(err)
	}
	b.Reset()
	if err := reencoded.Encode(b); err != nil {
		panic(err)
	}

	reencodedEnc := make([]byte, b.Len())
	copy(reencodedEnc, b.Bytes())

	if !bytes.Equal(originalEnc, reencodedEnc) {
		panic(fmt.Sprintf(
			"%q encoding/decoding/encoding mismatch\noriginal input:\n%s\nfirst encode:\n%s\nsecond encode:\n%s\n",
			original.BaseCmd.GetType(),
			hex.Dump(data),
			hex.Dump(originalEnc),
			hex.Dump(reencodedEnc),
		))
	}

	return 1
}
