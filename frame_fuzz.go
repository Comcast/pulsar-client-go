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
	if got := b.Bytes(); !bytes.Equal(data, got) {
		panic(fmt.Sprintf("re-encoded %q frame does not match original input:\noriginal:\n%s\nre-encoded:\n%s\n", f.BaseCmd.GetType(), hex.Dump(data), hex.Dump(got)))
	}

	return 1
}
