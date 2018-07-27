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
	"testing"
	"time"

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/Comcast/pulsar-client-go/frame"
	"github.com/golang/protobuf/proto"
)

func TestConnector(t *testing.T) {
	var ms mockSender

	dispatcher := newFrameDispatcher()
	c := newConnector(&ms, dispatcher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type response struct {
		success *api.CommandConnected
		err     error
	}
	resps := make(chan response, 1)

	go func() {
		var r response
		r.success, r.err = c.connect(ctx, "", "")
		resps <- r
	}()

	time.Sleep(100 * time.Millisecond)

	connected := api.CommandConnected{
		ServerVersion: proto.String("this is a test"),
	}
	f := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type:      api.BaseCommand_CONNECTED.Enum(),
			Connected: &connected,
		},
	}
	if err := dispatcher.notifyGlobal(f); err != nil {
		t.Fatalf("HandleConnected() err = %v; nil expected", err)
	}

	select {
	case resp := <-resps:
		if resp.err != nil {
			t.Fatalf("connector.connect() err: %v; nil expected", resp.err)
		}
		if !proto.Equal(resp.success, &connected) {
			t.Fatalf("connector.connect() response:\n%#v\nexpected:\n%#v", resp.success, connected)
		}
		t.Logf("connector.connect() response:\n%#v", resp.success)
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	if got, expected := len(ms.frames), 1; got != expected {
		t.Fatalf("got %d calls to sendSimpleCmd; expected %d", got, expected)
	}
}

func TestConnector_Timeout(t *testing.T) {
	var ms mockSender

	dispatcher := newFrameDispatcher()
	c := newConnector(&ms, dispatcher)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	type response struct {
		success *api.CommandConnected
		err     error
	}
	resps := make(chan response, 1)

	go func() {
		var r response
		r.success, r.err = c.connect(ctx, "", "")
		resps <- r
	}()

	// The context will timeout

	select {
	case resp := <-resps:
		if resp.err == nil {
			t.Fatalf("connector.connect() response:\n%#v; expected error instead", resp.success)
		}
		t.Logf("connector.connect() err: %v", resp.err)
	case <-time.After(2 * time.Second):
		t.Fatal("unexpected timeout")
	}

	if got, expected := len(ms.frames), 1; got != expected {
		t.Fatalf("got %d calls to sendSimpleCmd; expected %d", got, expected)
	}

	// There should be nothing in the dispatcher,
	// so `connected` should fail.
	f := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_CONNECTED.Enum(),
			Connected: &api.CommandConnected{
				ServerVersion: proto.String("1.2.3"),
			},
		},
	}
	err := dispatcher.notifyGlobal(f)
	if err == nil {
		t.Fatalf("connector.connected() err = %v; expected non-nil", err)
	}
	t.Logf("connector.connected() err = %v", err)
}

func TestConnector_Error(t *testing.T) {
	var ms mockSender

	dispatcher := newFrameDispatcher()
	c := newConnector(&ms, dispatcher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type response struct {
		success *api.CommandConnected
		err     error
	}
	resps := make(chan response, 1)

	go func() {
		var r response
		r.success, r.err = c.connect(ctx, "", "")
		resps <- r
	}()

	time.Sleep(100 * time.Millisecond)

	errorMsg := api.CommandError{
		RequestId: proto.Uint64(undefRequestID),
		Message:   proto.String("there was an error of sorts"),
	}
	f := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type:  api.BaseCommand_ERROR.Enum(),
			Error: &errorMsg,
		},
	}
	if err := dispatcher.notifyReqID(undefRequestID, f); err != nil {
		t.Fatalf("HandleReqID() err = %v; nil expected", err)
	}

	select {
	case resp := <-resps:
		if resp.err == nil {
			t.Fatalf("connector.connect() unexpected response:\n%#v\nexpected error", resp.err)
		}
		t.Logf("connector.connect() err: %v", resp.err)
	case <-time.After(time.Second):
		t.Fatal("unexpected timeout")
	}

	if got, expected := len(ms.frames), 1; got != expected {
		t.Fatalf("got %d calls to sendSimpleCmd; expected %d", got, expected)
	}

	// There should be nothing in the dispatcher,
	// so `connected` should fail.
	f = frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_CONNECTED.Enum(),
			Connected: &api.CommandConnected{
				ServerVersion: proto.String("this is a test"),
			},
		},
	}
	err := dispatcher.notifyGlobal(f)
	if err == nil {
		t.Fatalf("HandleConnected() err = %v; expected non-nil", err)
	}
	t.Logf("HandleConnected() err = %v", err)
}

func TestConnector_Outstanding(t *testing.T) {
	var ms mockSender

	c := newConnector(&ms, newFrameDispatcher())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// perform 1st connect
	go c.connect(ctx, "", "")

	time.Sleep(100 * time.Millisecond)

	// Additional attempts to connect while there's
	// an outstanding one should cause an error
	if _, err := c.connect(ctx, "", ""); err == nil {
		t.Fatalf("connector.connect() err = %v; expected non-nil because of outstanding request", err)
	} else {
		t.Logf("connector.connect() err = %v", err)
	}
}
