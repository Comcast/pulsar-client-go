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
	"hash"
	"hash/crc32"
)

// crc32cTbl holds the precomputed crc32 hash table
// used by Pulsar (crc32c)
var crc32cTbl = crc32.MakeTable(crc32.Castagnoli)

// frameChecksum handles computing the Frame checksum, both
// when decoding and encoding. The empty value is valid and
// represents no checksum. It is not thread-safe.
type frameChecksum struct {
	h hash.Hash32
}

// Write updates the hash with given bytes.
func (f *frameChecksum) Write(p []byte) (int, error) {
	if f.h == nil {
		f.h = crc32.New(crc32cTbl)
	}
	return f.h.Write(p)
}

// compute returns the computed checksum. If nothing
// was written to the checksum, nil is returned.
func (f *frameChecksum) compute() []byte {
	if f.h == nil {
		return nil
	}
	return f.h.Sum(nil)
}
