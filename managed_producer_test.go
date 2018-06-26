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
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/Comcast/pulsar-client-go/pulsartest"
	"github.com/golang/protobuf/proto"
)

func TestManagedProducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cp := NewManagedClientPool()
	mp := NewManagedProducer(cp, ManagedProducerConfig{
		ManagedClientConfig: ManagedClientConfig{
			ClientConfig: ClientConfig{
				Addr: srv.Addr,
			},
		},
		NewProducerTimeout: time.Second,
		Topic:              "test-topic",
	})

	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
		api.BaseCommand_LOOKUP,
		api.BaseCommand_PRODUCER,
	}
	if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}

	payload := []byte("hi")
	if _, err = mp.Send(ctx, payload); err != nil {
		t.Fatal(err)
	}

	select {
	case received := <-srv.Received:
		if got, expected := received.BaseCmd.GetType(), api.BaseCommand_SEND; got != expected {
			t.Fatalf("got frame type %q; expected %q", got, expected)
		}
		if got, expected := received.Payload, payload; !bytes.Equal(got, expected) {
			t.Fatalf("got payload %q; expected %q", got, expected)
		}

	case <-ctx.Done():
		t.Fatal("timeout waiting for SEND message")
	}
}

func TestManagedProducer_Redirect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	primarySrv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	topicSrv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	topic := "test"

	// Set primary server to redirect to topicSrv
	// for "test" topic
	primarySrv.SetTopicLookupResp(topic, topicSrv.Addr, api.CommandLookupTopicResponse_Connect, false)

	cp := NewManagedClientPool()
	mp := NewManagedProducer(cp, ManagedProducerConfig{
		ManagedClientConfig: ManagedClientConfig{
			ClientConfig: ClientConfig{
				Addr: primarySrv.Addr,
			},
		},
		NewProducerTimeout: time.Second,
		Topic:              topic,
	})

	primaryExpectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
		api.BaseCommand_LOOKUP,
	}
	if err = primarySrv.AssertReceived(ctx, primaryExpectedFrames...); err != nil {
		t.Fatal(err)
	}

	topicExpectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
		api.BaseCommand_PRODUCER,
	}
	if err = topicSrv.AssertReceived(ctx, topicExpectedFrames...); err != nil {
		t.Fatal(err)
	}

	payload := []byte("hi")
	if _, err = mp.Send(ctx, payload); err != nil {
		t.Fatal(err)
	}

	select {
	case received := <-topicSrv.Received:
		if got, expected := received.BaseCmd.GetType(), api.BaseCommand_SEND; got != expected {
			t.Fatalf("got frame type %q; expected %q", got, expected)
		}
		if got, expected := received.Payload, payload; !bytes.Equal(got, expected) {
			t.Fatalf("got payload %q; expected %q", got, expected)
		}

	case unexpected := <-primarySrv.Received:
		t.Fatalf("received unexpected frame from primary Pulsar server: %v", unexpected)

	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestManagedProducer_SrvClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cp := NewManagedClientPool()
	mp := NewManagedProducer(cp, ManagedProducerConfig{
		ManagedClientConfig: ManagedClientConfig{
			ClientConfig: ClientConfig{
				Addr: srv.Addr,
			},
		},
		NewProducerTimeout: time.Second,
		Topic:              "test-topic",
	})

	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
		api.BaseCommand_LOOKUP,
		api.BaseCommand_PRODUCER,
	}
	for i := 0; i < 3; i++ {
		if err = srv.CloseAll(); err != nil {
			t.Fatal(err)
		}
		if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
			t.Fatal(err)
		}
	}

	payload := []byte("hi")
	if _, err = mp.Send(ctx, payload); err != nil {
		t.Fatal(err)
	}

	select {
	case received := <-srv.Received:
		if got, expected := received.BaseCmd.GetType(), api.BaseCommand_SEND; got != expected {
			t.Fatalf("got frame type %q; expected %q", got, expected)
		}
		if got, expected := received.Payload, payload; !bytes.Equal(got, expected) {
			t.Fatalf("got payload %q; expected %q", got, expected)
		}

	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestManagedProducer_ProducerClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cp := NewManagedClientPool()
	mp := NewManagedProducer(cp, ManagedProducerConfig{
		ManagedClientConfig: ManagedClientConfig{
			ClientConfig: ClientConfig{
				Addr: srv.Addr,
			},
		},
		NewProducerTimeout: time.Second,
		Topic:              "test-topic",
	})

	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
	}
	if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		expectedFrames = []api.BaseCommand_Type{
			api.BaseCommand_LOOKUP,
		}
		if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
			t.Fatal(err)
		}

		// assert PRODUCER frame is received, then use it
		// to send CLOSE_PRODUCER to client
		select {
		case f := <-srv.Received:
			if got, expected := f.BaseCmd.GetType(), api.BaseCommand_PRODUCER; got != expected {
				t.Fatalf("got frame type %q; expected %q", got, expected)
			}

			// This will be sent to the client, closing the Producer.
			closeProducer := pulsartest.Frame{
				BaseCmd: &api.BaseCommand{
					Type: api.BaseCommand_CLOSE_PRODUCER.Enum(),
					CloseProducer: &api.CommandCloseProducer{
						ProducerId: f.BaseCmd.GetProducer().ProducerId,
						RequestId:  proto.Uint64(42),
					},
				},
			}
			if err = srv.Broadcast(closeProducer); err != nil {
				t.Fatal(err)
			}

		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	expectedFrames = []api.BaseCommand_Type{
		api.BaseCommand_LOOKUP,
		api.BaseCommand_PRODUCER,
	}
	if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}

	payload := []byte("hi")
	if _, err = mp.Send(ctx, payload); err != nil {
		t.Fatal(err)
	}

	select {
	case received := <-srv.Received:
		if got, expected := received.BaseCmd.GetType(), api.BaseCommand_SEND; got != expected {
			t.Fatalf("got frame type %q; expected %q", got, expected)
		}
		if got, expected := received.Payload, payload; !bytes.Equal(got, expected) {
			t.Fatalf("got payload %q; expected %q", got, expected)
		}

	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}
