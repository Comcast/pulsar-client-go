/**
* Copyright 2018 Comcast Cable Communications Management, LLC
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package pulsar

import (
	"context"
	"fmt"
	"net"
)

// mockPulsarServer emulates a Pulsar server
type mockPulsarServer struct {
	addr  string
	errs  chan error
	conns chan *conn
}

func newMockPulsarServer(ctx context.Context) (*mockPulsarServer, error) {
	l, err := net.ListenTCP("tcp4", &net.TCPAddr{
		IP:   net.IPv4zero,
		Port: 0,
	})
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		l.Close()
	}()

	mock := mockPulsarServer{
		addr:  fmt.Sprintf("pulsar://%s", l.Addr().String()),
		errs:  make(chan error),
		conns: make(chan *conn, 1),
	}

	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				mock.errs <- err
				return
			}

			// close all connections when
			// context is canceled
			go func() {
				<-ctx.Done()
				c.Close()
			}()

			mock.conns <- &conn{
				rc:      c,
				w:       c,
				closedc: make(chan struct{}),
			}
		}
	}()

	return &mock, nil
}
