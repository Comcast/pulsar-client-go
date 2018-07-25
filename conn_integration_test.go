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

func TestConn_Int_Connect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := newTCPConn(pulsarAddr(t), time.Second)
	if err != nil {
		t.Fatal(err)
	}

	responses := make(chan frame.Frame)
	readErr := make(chan error, 1)
	go func() {
		readErr <- c.read(func(f frame.Frame) {
			responses <- f
		})
	}()

	connect := api.BaseCommand{
		Type: api.BaseCommand_CONNECT.Enum(),
		Connect: &api.CommandConnect{
			ClientVersion:   proto.String("go-client-test"),
			AuthMethod:      api.AuthMethod_AuthMethodNone.Enum(),
			ProtocolVersion: proto.Int32(ProtoVersion),
		},
	}

	if err := c.sendSimpleCmd(connect); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())

	case err := <-readErr:
		t.Fatalf("read error: %v", err)

	case resp := <-responses:
		respType := resp.BaseCmd.GetType()
		t.Logf("Received %q message", respType.String())

		switch respType {

		case api.BaseCommand_CONNECTED:
			connected := resp.BaseCmd.GetConnected()
			t.Logf("ProtocolVersion = %d, ServerVersion = %q", connected.GetProtocolVersion(), connected.GetServerVersion())

		case api.BaseCommand_ERROR:
			err := resp.BaseCmd.GetError()
			t.Fatalf("unexpected error message from server: %q", err.GetMessage())

		}
	}
}
