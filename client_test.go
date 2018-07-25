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
	"context"
	"io"
	"testing"
	"time"

	"github.com/Comcast/pulsar-client-go/frame"
)

func TestClient_ServerInitiatedClose(t *testing.T) {
	ctx, srvClose := context.WithCancel(context.Background())
	defer srvClose()
	srv, err := newMockPulsarServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	asyncErrs := make(chan error, 8)
	c, err := NewClient(ClientConfig{
		Addr: srv.addr,
		Errs: asyncErrs,
	})
	if err != nil {
		t.Fatal(err)
	}

	// wait for the Pulsar server to accept the connection
	var srvConn *conn
	select {
	case srvConn = <-srv.conns:
		t.Log("server received connection")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server to receive connection")
	}
	// terminate the connection from the server's end
	if err := srvConn.close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-asyncErrs:
		if expected := io.EOF; err != expected {
			t.Fatalf("Client.Errors() received unexpected err = %v; expected %v", err, expected)
		}
		t.Logf("Client.Errors() received (expected) err = %v", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Client.Errors() to receive an error")
	}

	select {
	case <-c.Closed():
		t.Log("Client.OnClose() unblocked")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Client.OnClose() to unblock")
	}
}

func TestClient_ClientInitiatedClose(t *testing.T) {
	ctx, srvClose := context.WithCancel(context.Background())
	defer srvClose()
	srv, err := newMockPulsarServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	asyncErrs := make(chan error, 8)
	c, err := NewClient(ClientConfig{
		Addr: srv.addr,
		Errs: asyncErrs,
	})
	if err != nil {
		t.Fatal(err)
	}

	// wait for the Pulsar server to accept the connection
	var srvConn *conn
	select {
	case srvConn = <-srv.conns:
		t.Log("server received connection")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server to receive connection")
	}

	// start reading from the remote conn.
	// send read errors to srvConnReadErr chan
	srvConnReadErr := make(chan error, 1)
	go func() {
		srvConnReadErr <- srvConn.read(func(f frame.Frame) {})
	}()

	// terminate the connection from the client's end
	if err := c.Close(); err != nil {
		t.Fatalf("Client.Close() err = %v; expected nil", err)
	}

	select {
	case err := <-asyncErrs:
		t.Logf("Client.Errors() received (expected) err = %v", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Client.Errors() to receive an error")
	}

	select {
	case <-c.Closed():
		t.Log("Client.OnClose() unblocked")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Client.OnClose() to unblock")
	}

	select {
	case err := <-srvConnReadErr:
		t.Logf("remote conn read() unblocked with (expected) err = %v", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for remote conn read() to unblock")
	}
}
