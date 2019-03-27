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
	"crypto/tls"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/Comcast/pulsar-client-go/frame"
)

// newTCPConn creates a conn using a TCPv4 connection to the given
// (pulsar server) address.
func newTCPConn(addr string, timeout time.Duration) (*conn, error) {
	if strings.HasPrefix(addr, "pulsar://") {
		addr = strings.TrimPrefix(addr, "pulsar://")
	} else if strings.HasPrefix(addr, "pulsar+ssl://") {
		addr = strings.TrimPrefix(addr, "pulsar+ssl://")
	}

	d := net.Dialer{
		DualStack: false,
		Timeout:   timeout,
	}
	c, err := d.Dial("tcp4", addr)
	if err != nil {
		return nil, err
	}

	return &conn{
		rc:      c,
		w:       c,
		closedc: make(chan struct{}),
	}, nil
}

// newTLSConn creates a conn using a TCPv4+TLS connection to the given
// (pulsar server) address.
func newTLSConn(addr string, tlsCfg *tls.Config, timeout time.Duration) (*conn, error) {
	if strings.HasPrefix(addr, "pulsar://") {
		addr = strings.TrimPrefix(addr, "pulsar://")
	} else if strings.HasPrefix(addr, "pulsar+ssl://") {
		addr = strings.TrimPrefix(addr, "pulsar+ssl://")
	}

	d := net.Dialer{
		DualStack: false,
		Timeout:   timeout,
	}
	c, err := tls.DialWithDialer(&d, "tcp4", addr, tlsCfg)
	if err != nil {
		return nil, err
	}

	return &conn{
		rc:      c,
		w:       c,
		closedc: make(chan struct{}),
	}, nil
}

// conn is responsible for writing and reading
// Frames to and from the underlying connection (r and w).
type conn struct {
	rc io.ReadCloser

	wmu sync.Mutex // protects w to ensure frames aren't interleaved
	w   io.Writer

	cmu      sync.Mutex // protects following
	isClosed bool
	closedc  chan struct{}
}

// close closes the underlaying connection.
// This will cause read() to unblock and return
// an error. It will also cause the closed channel
// to unblock.
func (c *conn) close() error {
	c.cmu.Lock()
	defer c.cmu.Unlock()

	if c.isClosed {
		return nil
	}

	err := c.rc.Close()
	close(c.closedc)
	c.isClosed = true

	return err
}

// closed returns a channel that will unblock
// when the connection has been closed and is no
// longer usable.
func (c *conn) closed() <-chan struct{} {
	return c.closedc
}

// read blocks while it reads from r until an error occurs.
// It passes all frames to the provided handler, sequentially
// and from the same goroutine as called with. Any error encountered
// will close the connection. Also if close() is called,
// read() will unblock. Once read returns, the conn should
// be considered unusable.
func (c *conn) read(frameHandler func(f frame.Frame)) error {
	for {
		var f frame.Frame
		if err := f.Decode(c.rc); err != nil {
			// It's very possible that the connection is already closed at this
			// point, since any connection closed errors would bubble up
			// from Decode. But just in case it's a decode error (bad data for example),
			// we attempt to close the connection. Any error is ignored
			// since the Decode error is the primary one.
			_ = c.close()
			return err
		}
		frameHandler(f)
	}
}

// sendSimpleCmd writes a "simple" frame to the wire. It
// is safe to use concurrently.
func (c *conn) sendSimpleCmd(cmd api.BaseCommand) error {
	return c.writeFrame(&frame.Frame{
		BaseCmd: &cmd,
	})
}

// sendPayloadCmd writes a "payload" frame to the wire. It
// is safe to use concurrently.
func (c *conn) sendPayloadCmd(cmd api.BaseCommand, metadata api.MessageMetadata, payload []byte) error {
	return c.writeFrame(&frame.Frame{
		BaseCmd:  &cmd,
		Metadata: &metadata,
		Payload:  payload,
	})
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, frame.MaxFrameSize))
	},
}

// writeFrame encodes the given frame and writes
// it to the wire in a thread-safe manner.
func (c *conn) writeFrame(f *frame.Frame) error {
	b := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(b)
	b.Reset()

	if err := f.Encode(b); err != nil {
		return err
	}

	c.wmu.Lock()
	_, err := b.WriteTo(c.w)
	c.wmu.Unlock()

	return err
}
