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
)

func TestPinger_HandlePing(t *testing.T) {
	var ms mockSender

	dispatcher := newFrameDispatcher()
	c := newPinger(&ms, dispatcher)
	if err := c.handlePing(api.BaseCommand_PING, &api.CommandPing{}); err != nil {
		t.Fatalf("pinger.handlePing() err = %v; nil expected", err)
	}

	if got, expected := len(ms.frames), 1; got != expected {
		t.Fatalf("pinger.handlePing() resulted in %d commands sent; expected %d", got, expected)
	}

	sent := ms.frames[0].BaseCmd
	if got, expected := sent.GetType(), api.BaseCommand_PONG; got != expected {
		t.Fatalf("pinger.handlePing() sent message of type %q; expected %q", got, expected)
	}
}

func TestPinger_Ping(t *testing.T) {
	var ms mockSender

	dispatcher := newFrameDispatcher()
	c := newPinger(&ms, dispatcher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pingResp := make(chan error, 1)
	go func() {
		pingResp <- c.ping(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	f := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_PONG.Enum(),
			Pong: &api.CommandPong{},
		},
	}
	if err := dispatcher.notifyGlobal(f); err != nil {
		t.Fatalf("pinger.handleGlobal() err = %v; nil expected", err)
	}

	err := <-pingResp
	if err != nil {
		t.Fatalf("pinger.ping() err = %v; nil expected", err)
	}

	if got, expected := len(ms.frames), 1; got != expected {
		t.Fatalf("pinger.ping() resulted in %d commands sent; expected %d", got, expected)
	}
}

func TestPinger_Outstanding(t *testing.T) {
	var ms mockSender

	dispatcher := newFrameDispatcher()
	c := newPinger(&ms, dispatcher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.ping(ctx)

	time.Sleep(100 * time.Millisecond)

	if err := c.ping(ctx); err == nil {
		t.Fatalf("pinger.ping() err = %v; non-nil expected because of outstanding ping", err)
	} else {
		t.Logf("pinger.ping() err = %v because of outstanding ping", err)
	}

	cancel()

	if got, expected := len(ms.getFrames()), 1; got != expected {
		t.Fatalf("pinger.ping() resulted in %d commands sent; expected %d", got, expected)
	}
}
