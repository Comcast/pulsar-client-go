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
	"github.com/Comcast/pulsar-client-go/pulsartest"
)

func TestManagedClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: srv.Addr,
		},
	})
	defer mc.Stop()

	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
	}
	if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}

	c, err := mc.Get(ctx)
	if err != nil {
		t.Fatalf("Get() err = %v; expected nil", err)
	}

	select {
	case <-c.Closed():
		t.Fatal("client is closed; expected not to be")
	default:
	}
}

func TestManagedClient_SrvClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: srv.Addr,
		},
	})
	defer mc.Stop()

	// repeatedly close the connection from the server's end;
	// ensure the client reconnects
	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
	}
	for i := 0; i < 3; i++ {
		if err = srv.CloseAll(); err != nil {
			t.Fatal(err)
		}
		if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
			t.Fatal(err)
		}
	}

	c, err := mc.Get(ctx)
	if err != nil {
		t.Fatalf("Get() err = %v; nil expected", err)
	}

	select {
	case <-c.Closed():
		t.Fatal("client is closed; expected to not be")
	default:
	}
}

func TestManagedClient_PingFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Set server to not respond to PING messages
	srv.SetIgnorePings(true)

	// Set client to ping every 1/2 second
	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: srv.Addr,
		},
		PingFrequency: 500 * time.Millisecond,
	})
	defer mc.Stop()

	// The client should reconnect indefinitely, since
	// no pings will be answered. Here we check 3 times
	// that it tried to connect.
	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
		api.BaseCommand_PING,
	}
	for i := 0; i < 3; i++ {
		if err := srv.AssertReceived(ctx, expectedFrames...); err != nil {
			t.Fatal(err)
		}
	}
}

func TestManagedClient_ConnectFailure(t *testing.T) {
	timeout := 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Set server to NOT respond to CONNECT messages,
	// then later enable them
	srv.SetIgnoreConnects(true)

	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: srv.Addr,
		},
		ConnectTimeout: time.Second, // shorten connect timeout since no CONNECT response is expected
	})
	defer mc.Stop()

	getCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	// Client shouldn't be able to connect
	if _, err := mc.Get(getCtx); err == nil {
		t.Fatalf("Get() err = %v; expected non-nil", err)
	} else {
		t.Logf("Get() err (expected) = %v", err)
	}

	go func() {
		// After 1 second, enable CONNECT
		// messages again. This should allow
		// the client to connect successfully
		<-time.After(time.Second)
		srv.SetIgnoreConnects(false)
	}()

	// Now client should be able to connect
	if _, err := mc.Get(ctx); err != nil {
		t.Fatalf("Get() err = %v; expected nil", err)
	}
}

func TestManagedClient_Stop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mc := NewManagedClient(ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: srv.Addr,
		},
	})
	defer mc.Stop()

	// wait for the client to connect
	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
	}
	if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}

	c, err := mc.Get(ctx)
	if err != nil {
		t.Fatalf("Get() err = %v; nil expected", err)
	}

	select {
	case <-c.Closed():
		t.Fatal("client is closed; expected to not be")
	default:
	}

	if err = mc.Stop(); err != nil {
		t.Fatal(err)
	}

	// Assert client is closed
	select {
	case <-c.Closed():
	case <-time.After(time.Second):
		t.Fatal("client is NOT closed; expected to be")
	}

	// Assert ManagedClient does not try to re-connect
	select {
	case f := <-srv.Received:
		t.Fatalf("server received unexpected frame: %v", f)
	case <-time.After(time.Second):
	}

	// Assert Get returns an error
	_, err = mc.Get(ctx)
	if err == nil {
		t.Fatalf("Get() err = %v; non-nil expected", err)
	}
	t.Logf("Get() err (expected) = %v", err)
}
