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
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/golang/protobuf/proto"
)

func TestFrameDecode_Simple(t *testing.T) {
	wire := `
00000000  00 00 00 19 00 00 00 15  08 03 1a 11 0a 0d 50 75  |..............Pu|
00000010  6c 73 61 72 20 53 65 72  76 65 72 10 09           |lsar Server..|
`

	b := bytes.NewReader(hexUndump(wire))

	var f Frame
	if err := f.Decode(b); err != nil {
		t.Fatal(err)
	}

	expectedMsg := &api.BaseCommand{
		Type: api.BaseCommand_CONNECTED.Enum(),
		Connected: &api.CommandConnected{
			ProtocolVersion: proto.Int32(9),
			ServerVersion:   proto.String("Pulsar Server"),
		},
	}

	if !proto.Equal(f.BaseCmd, expectedMsg) {
		t.Fatalf("got message: %v\nexpected %v", f.BaseCmd, expectedMsg)
	}

	if f.Metadata != nil {
		t.Fatalf("got Frame with metadata; expected nil")
	}
	if len(f.Payload) != 0 {
		t.Fatalf("got from with payload of len %d; expected 0", len(f.Payload))
	}
}

func TestFrameDecode_Payload(t *testing.T) {
	wire := `
00000000  00 00 00 27 00 00 00 0d  08 09 4a 09 08 2a 12 05  |...'......J..*..|
00000010  08 02 10 d2 02 00 00 00  0d 0a 02 67 6f 10 00 18  |...........go...|
00000020  a8 f9 d2 bb 84 2c 68 69  3a 20 30                 |.....,hi: 0|
`

	b := bytes.NewReader(hexUndump(wire))

	var f Frame
	if err := f.Decode(b); err != nil {
		t.Fatal(err)
	}

	expectedMsg := &api.BaseCommand{
		Type: api.BaseCommand_MESSAGE.Enum(),
		Message: &api.CommandMessage{
			ConsumerId: proto.Uint64(42),
			MessageId: &api.MessageIdData{
				LedgerId: proto.Uint64(2),
				EntryId:  proto.Uint64(338),
			},
		},
	}

	expectedMeta := &api.MessageMetadata{
		ProducerName: proto.String("go"),
		SequenceId:   proto.Uint64(0),
		PublishTime:  proto.Uint64(1513027321000),
	}

	expectedPayload := []byte("hi: 0")

	if !proto.Equal(f.BaseCmd, expectedMsg) {
		t.Fatalf("got message: %v\nexpected %v", f.BaseCmd, expectedMsg)
	}

	if !proto.Equal(f.Metadata, expectedMeta) {
		t.Fatalf("got metadata: %v\nexpected %v", f.Metadata, expectedMeta)
	}

	if !bytes.Equal(f.Payload, expectedPayload) {
		t.Fatalf("got payload: %q\nexpected %q", f.Payload, expectedPayload)
	}
}

func TestFrameDecode_UnexpectedEOF(t *testing.T) {
	// truncated last byte
	wire := `
00000000  00 00 00 27 00 00 00 0d  08 09 4a 09 08 2a 12 05  |...'......J..*..|
00000010  08 02 10 d2 02 00 00 00  0d 0a 02 67 6f 10 00 18  |...........go...|
00000020  a8 f9 d2 bb 84 2c 68 69  3a 20                    |.....,hi: 0|
`
	b := bytes.NewReader(hexUndump(wire))

	var f Frame
	err := f.Decode(b)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("frame.Decode() err = %v; expected %v", err, io.ErrUnexpectedEOF)
	}
	t.Logf("frame.Decode() = %v", err)
}

func TestFrameDecode_MaxSize(t *testing.T) {
	// large command size value
	wire := `
00000000  FF FF FF FF 00 00 00 0d  08 09 4a 09 08 2a 12 05  |...'......J..*..|
00000010  08 02 10 d2 02 00 00 00  0d 0a 02 67 6f 10 00 18  |...........go...|
00000020  a8 f9 d2 bb 84 2c 68 69  3a 20                    |.....,hi: 0|
`
	b := bytes.NewReader(hexUndump(wire))

	var f Frame
	err := f.Decode(b)
	if err == nil {
		t.Fatalf("frame.Decode() err = %v; non-nil expected", err)
	}
	t.Logf("frame.Decode() = %v", err)
}

func TestFrameEncode_Simple(t *testing.T) {
	wire := `
00000000  00 00 00 19 00 00 00 15  08 03 1a 11 0a 0d 50 75  |..............Pu|
00000010  6c 73 61 72 20 53 65 72  76 65 72 10 09           |lsar Server..|
`
	expected := hexUndump(wire)

	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_CONNECTED.Enum(),
			Connected: &api.CommandConnected{
				ProtocolVersion: proto.Int32(9),
				ServerVersion:   proto.String("Pulsar Server"),
			},
		},
	}

	var out bytes.Buffer
	if err := f.Encode(&out); err != nil {
		t.Fatalf("Frame.Encode() err = %v; nil expected", err)
	}
	got := out.Bytes()

	if !bytes.Equal(got, expected) {
		t.Fatalf("Frame.Encode():\n%s\nexpected:\n%s", hex.Dump(got), hex.Dump(expected))
	}
}

func TestFrameEncode_Payload(t *testing.T) {
	wire := `
0000000 00 00 00 3f 00 00 00 0c 08 09 4a 08 08 00 12 04
0000010 08 00 10 01 0e 01 a8 50 cd 91 00 00 00 19 0a 0e
0000020 73 74 61 6e 64 61 6c 6f 6e 65 2d 30 2d 31 10 00
0000030 18 c1 b1 df a9 ca 2c 48 65 6c 6c 6f 20 77 6f 72
0000040 6c 64 21
`
	expected := hexUndump(wire)

	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_MESSAGE.Enum(),
			Message: &api.CommandMessage{
				ConsumerId: proto.Uint64(0),
				MessageId: &api.MessageIdData{
					LedgerId: proto.Uint64(0),
					EntryId:  proto.Uint64(1),
				},
			},
		},
		Metadata: &api.MessageMetadata{
			ProducerName: proto.String("standalone-0-1"),
			SequenceId:   proto.Uint64(0),
			PublishTime:  proto.Uint64(1531780257985),
		},
		Payload: []byte("Hello world!"),
	}

	var out bytes.Buffer
	if err := f.Encode(&out); err != nil {
		t.Fatalf("Frame.Encode() err = %v; nil expected", err)
	}
	got := out.Bytes()

	if !bytes.Equal(got, expected) {
		t.Fatalf("Frame.Encode():\n%s\nexpected:\n%s", hex.Dump(got), hex.Dump(expected))
	}
}

func TestFrame_Captured(t *testing.T) {
	// Read Pulsar frames captured off the wire. Ensure that
	// they can be decoded and then re-encoded.
	inputs, err := filepath.Glob("testdata/frames/*.frame")
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range inputs {
		f := f
		t.Run(f, func(t *testing.T) {
			fd, err := os.Open(f)
			if err != nil {
				t.Fatalf("unable to decode frame: %v", err)
			}
			defer fd.Close()

			var b bytes.Buffer

			var original Frame
			if err := original.Decode(io.TeeReader(fd, &b)); err != nil {
				t.Fatal(err)
			}

			consumed := make([]byte, b.Len())
			copy(consumed, b.Bytes())

			b.Reset()
			if err := original.Encode(&b); err != nil {
				t.Fatal(err)
			}

			if reencoded := b.Bytes(); !bytes.Equal(consumed, reencoded) {
				t.Fatalf(
					"original bytes don't match re-encoded:\n%s\n%s\n",
					hex.Dump(consumed),
					hex.Dump(reencoded),
				)
			}
		})
	}
}

func TestFrameEncode_MaxFrameSize(t *testing.T) {
	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_MESSAGE.Enum(),
			Message: &api.CommandMessage{
				ConsumerId: proto.Uint64(42),
				MessageId: &api.MessageIdData{
					LedgerId: proto.Uint64(2),
					EntryId:  proto.Uint64(338),
				},
			},
		},
		Metadata: &api.MessageMetadata{
			ProducerName: proto.String("go"),
			SequenceId:   proto.Uint64(0),
			PublishTime:  proto.Uint64(1513027321000),
		},
		Payload: make([]byte, maxFrameSize), // payload + metadata + baseCmd will be > maxFrameSize
	}

	var out bytes.Buffer
	err := f.Encode(&out)
	if err == nil {
		t.Fatalf("Frame.Encode() err = %v; non-nil", err)
	}
	t.Logf("Frame.Encode() err = %v", err)
}
