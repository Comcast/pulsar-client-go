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

package pulsartest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/golang/protobuf/proto"
)

// ==============
// NOTE: Frame is duplicated from the pulsar package. This avoids
// cyclical imports.
// ==============

// maxFrameSize is defined by the Pulsar spec with a single
// sentence: "The maximum allowable size of a single frame is 5 MB."
//
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Framing-5l6bym
const maxFrameSize = 5 * 1024 * 1024 // 5mb

// magicNumber is a 2-byte byte array (0x0e01)
// identifying an optional checksum in the message,
// as defined by the pulsar protocol
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Payloadcommands-kbk8xf
var magicNumber = []byte{0x0e, 0x01}

// Frame represents a pulsar message frame.
// It can be used to encode and decode messages
// to and from the Pulsar binary wire format.
//
// The binary protocol is outlined here:
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/
// But the Java source should be considered the canonical format.
//
// All sizes are passed as 4-byte unsigned big endian integers.
//
// "Simple" command frame format:
//
//	 +------------------------------------------------------------------------+
//	 | totalSize (uint32) | commandSize (uint32) | message (protobuf encoded) |
//	 |       4 bytes      |       4 bytes        |         var length         |
//	 |====================|======================|============================|
//	 | size of everything | size of the message  |                            |
//	 | following these 4  |                      |                            |
//	 | bytes              |                      |                            |
//	 +------------------------------------------------------------------------+
//
// "Payload" command frame format (It has the same 3 fields as a "simple" command, plus the following):
//
//	 +-------------------------------------------------------------------------------------------------------------------------------------------------+
//	 | "Simple" fields | magicNumber (0x0e01) | checksum (CRC32-C) | metadataSize (uint32) | metadata (protobuf encoded) |       payload (bytes)       |
//	 |   var length    |        2 bytes       |       4 bytes      |       4 bytes         |          var length         |   totalSize - (SUM others)  |
//	 |=================|======================|====================|=======================|=============================|=============================|
//	 |                 | OPTIONAL If present, | OPTIONAL Checksum  | size of the metadata  |                             | Any sequence of bytes,      |
//	 |                 | indicates following  | of the following   |                       |                             | possibly compressed and     |
//	 |                 | 4 bytes are checksum | bytes              |                       |                             | or encrypted (see metadata) |
//	 +-------------------------------------------------------------------------------------------------------------------------------------------------+
//
type Frame struct {
	// BaseCmd is a required field
	BaseCmd *api.BaseCommand

	// The following fields are optional.
	// If present, the frame is a "Payload"
	// command, as opposed to a "Simple" command
	// if there's only the BaseCmd.
	Metadata *api.MessageMetadata
	Payload  []byte
}

// Equal returns true if the other Frame is
// equal to the receiver frame, false otherwise.
func (f *Frame) Equal(other Frame) bool {
	if !proto.Equal(f.BaseCmd, other.BaseCmd) {
		return false
	}

	if !proto.Equal(f.Metadata, other.Metadata) {
		return false
	}

	return bytes.Equal(f.Payload, other.Payload)
}

// Decode the pulsar binary protocol from r into
// the receiver frame. Returns any errors encountered.
func (f *Frame) Decode(r io.Reader) error {
	var err error

	// reusable buffer for 4-byte uint32s
	sizeBuf := make([]byte, 4)

	// Read totalSize
	// totalSize: The size of the frame,
	// counting everything that comes after it (in bytes)
	if _, err = io.ReadFull(r, sizeBuf); err != nil {
		return err
	}
	totalSize := binary.BigEndian.Uint32(sizeBuf)

	// frameSize is the total length of the frame (totalSize
	// is the size of all the _following_ bytes).
	frameSize := int(totalSize) + 4
	// ensure reasonable frameSize
	if frameSize > maxFrameSize {
		return fmt.Errorf("frame size (%d) cannot be greater than max frame size (%d)", frameSize, maxFrameSize)
	}

	// Wrap our reader so that we can only read
	// bytes from our frame
	lr := &io.LimitedReader{
		N: int64(totalSize),
		R: r,
	}

	// Read cmdSize
	if _, err = io.ReadFull(lr, sizeBuf); err != nil {
		return err
	}
	cmdSize := binary.BigEndian.Uint32(sizeBuf)

	// Read protobuf encoded BaseCommand
	cmdBuf := make([]byte, cmdSize)
	if _, err = io.ReadFull(lr, cmdBuf); err != nil {
		return err
	}
	f.BaseCmd = new(api.BaseCommand)
	if err = proto.Unmarshal(cmdBuf, f.BaseCmd); err != nil {
		return err
	}

	// There are 3 possibilities for the following fields:
	//  - EOF: If so, this is a "simple" command. No more parsing required.
	//  - 2-byte magic number: Indicates the following 4 bytes are a checksum
	//  - 4-byte metadata size

	// The message may optionally stop here. If so,
	// this is a "simple" command.
	if lr.N <= 0 {
		return nil
	}

	// Optionally, the next 2 bytes may be the magicNumber. If
	// so, it indicates that the following 4 bytes are a checksum.
	// If not, the following 2 bytes (plus the 2 bytes already read),
	// are the metadataSize, which is why a 4 byte buffer is used.
	if _, err = io.ReadFull(lr, sizeBuf); err != nil {
		return err
	}

	// Check for magicNumber and checksum
	var hasChecksum bool
	var checksum [4]byte
	if magicNumber[0] == sizeBuf[0] && magicNumber[1] == sizeBuf[1] {
		hasChecksum = true

		// We already read the 2-byte magicNumber + 2 additional bytes
		// of the checksum
		checksum[0] = sizeBuf[2]
		checksum[1] = sizeBuf[3]

		if _, err = io.ReadFull(lr, checksum[2:]); err != nil {
			return err
		}
	}
	// TODO: use checksum to verify payload
	_ = hasChecksum

	if hasChecksum {
		if _, err = io.ReadFull(lr, sizeBuf); err != nil {
			return err
		}
	}

	// Read metadataSize
	metadataSize := binary.BigEndian.Uint32(sizeBuf)

	// Read protobuf encoded metadata
	metaBuf := make([]byte, metadataSize)
	if _, err = io.ReadFull(lr, metaBuf); err != nil {
		return err
	}
	f.Metadata = new(api.MessageMetadata)
	if err = proto.Unmarshal(metaBuf, f.Metadata); err != nil {
		return err
	}

	// Anything left in the frame is considered
	// the payload and can be any sequence of bytes.
	if lr.N > 0 {
		f.Payload = make([]byte, lr.N)
		if _, err = io.ReadFull(lr, f.Payload); err != nil {
			return err
		}
	}

	return nil
}

// Encode writes the pulsar binary protocol encoded
// frame into w.
func (f *Frame) Encode(w io.Writer) error {
	// encode baseCommand
	encodedBaseCmd, err := proto.Marshal(f.BaseCmd)
	if err != nil {
		return err
	}
	cmdSize := uint32(len(encodedBaseCmd))

	var metadataSize uint32
	var encodedMetadata []byte
	// Check if this is a "simple" command, ie
	// no metadata nor payload
	if f.Metadata != nil {
		if encodedMetadata, err = proto.Marshal(f.Metadata); err != nil {
			return err
		}
		metadataSize = uint32(len(encodedMetadata))
	}

	//
	// | totalSize (4) | cmdSize (4) | cmd (...) | metadataSize (4) | metadata (...) | payload (...) |
	//
	totalSize := cmdSize + 4
	if metadataSize > 0 {
		totalSize += metadataSize + 4 + uint32(len(f.Payload))
	}

	frameSize := totalSize + 4
	if frameSize > maxFrameSize {
		return fmt.Errorf("encoded frame size (%d bytes) is larger than max allowed frame size (%d bytes)", frameSize, maxFrameSize)
	}

	// write totalSize
	if err = binary.Write(w, binary.BigEndian, totalSize); err != nil {
		return err
	}

	// write cmdSize
	if err = binary.Write(w, binary.BigEndian, cmdSize); err != nil {
		return err
	}

	// write baseCommand
	buf := bytes.NewReader(encodedBaseCmd)
	if _, err = io.Copy(w, buf); err != nil {
		return err
	}

	if metadataSize == 0 {
		// this is a "simple" command
		// (no metadata, payload)
		return nil
	}

	// write metadataSize
	if err = binary.Write(w, binary.BigEndian, metadataSize); err != nil {
		return err
	}

	// write metadata
	buf.Reset(encodedMetadata)
	if _, err = io.Copy(w, buf); err != nil {
		return err
	}

	// write payload
	buf.Reset(f.Payload)
	_, err = io.Copy(w, buf)
	return err
}
