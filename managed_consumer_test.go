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
	"fmt"
	"testing"
	"time"

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/Comcast/pulsar-client-go/frame"
	"github.com/Comcast/pulsar-client-go/pulsartest"
	"github.com/golang/protobuf/proto"
)

func TestManagedConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cp := NewManagedClientPool()
	mc := NewManagedConsumer(cp, ManagedConsumerConfig{
		ManagedClientConfig: ManagedClientConfig{
			ClientConfig: ClientConfig{
				Addr: srv.Addr,
			},
		},
		NewConsumerTimeout: time.Second,
		Topic:              "test-topic",
		Name:               "test",
		Exclusive:          false,
	})

	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
		api.BaseCommand_LOOKUP,
	}
	if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}

	var consumerID uint64
	select {
	case f := <-srv.Received:
		if got, expected := f.BaseCmd.GetType(), api.BaseCommand_SUBSCRIBE; got != expected {
			t.Fatalf("got frame type %q; expected %q", got, expected)
		}
		consumerID = f.BaseCmd.GetSubscribe().GetConsumerId()

	case <-time.After(time.Second):
		t.Fatal("timeout waiting for SUBSCRIBE message")
	}

	// Send message to consumer
	payload := []byte("hola mundo")

	message := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_MESSAGE.Enum(),
			Message: &api.CommandMessage{
				ConsumerId: proto.Uint64(consumerID),
				MessageId: &api.MessageIdData{
					EntryId:  proto.Uint64(1),
					LedgerId: proto.Uint64(1),
				},
			},
		},
		Metadata: &api.MessageMetadata{
			ProducerName: proto.String("something"),
			SequenceId:   proto.Uint64(42),
			PublishTime:  proto.Uint64(12345),
		},
		Payload: payload,
	}
	if err = srv.Broadcast(message); err != nil {
		t.Fatal(err)
	}

	msg, err := mc.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive() err = %v; nil expected", err)
	}

	if got, expected := string(msg.Payload), string(payload); got != expected {
		t.Fatalf("Receive() message payload = %q; expected %q\n%#v", got, expected, msg)
	}
	t.Logf("Receive() message payload = %q", msg.Payload)
}

func TestManagedConsumer_ReceiveAsync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	queueSize := 4

	cp := NewManagedClientPool()
	mc := NewManagedConsumer(cp, ManagedConsumerConfig{
		ManagedClientConfig: ManagedClientConfig{
			ClientConfig: ClientConfig{
				Addr: srv.Addr,
			},
		},
		NewConsumerTimeout: time.Second,
		Topic:              "test-topic",
		Name:               "test",
		Exclusive:          false,
		QueueSize:          queueSize,
	})

	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
		api.BaseCommand_LOOKUP,
	}
	if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}

	// intercept subscribe command to get consumerID
	var consumerID uint64
	select {
	case f := <-srv.Received:
		if got, expected := f.BaseCmd.GetType(), api.BaseCommand_SUBSCRIBE; got != expected {
			t.Fatalf("got frame type %q; expected %q", got, expected)
		}
		consumerID = f.BaseCmd.GetSubscribe().GetConsumerId()

	case <-time.After(time.Second):
		t.Fatal("timeout waiting for SUBSCRIBE message")
	}

	received := make(chan Message, queueSize+1)
	go mc.ReceiveAsync(ctx, received)

	// send messages to consumer
	sent := make([]frame.Frame, queueSize+1)
	for i := range sent {
		// Send message to consumer
		payload := []byte(fmt.Sprintf("%d hola mundo", i))
		sent[i] = frame.Frame{
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_MESSAGE.Enum(),
				Message: &api.CommandMessage{
					ConsumerId: proto.Uint64(consumerID),
					MessageId: &api.MessageIdData{
						EntryId:  proto.Uint64(uint64(i)),
						LedgerId: proto.Uint64(1),
					},
				},
			},
			Metadata: &api.MessageMetadata{
				ProducerName: proto.String("something"),
				SequenceId:   proto.Uint64(uint64(i)),
				PublishTime:  proto.Uint64(12345),
			},
			Payload: payload,
		}

	}

	errs := make(chan error, 1)
	go func() {
		for _, f := range sent {
			if err := srv.Broadcast(f); err != nil {
				errs <- err
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()

	// pull messages from receiver chan
	var got []string
RECEIVE:
	for {
		select {
		case f := <-received:
			got = append(got, string(f.Payload))
			if len(got) == len(sent) {
				break RECEIVE
			}

		case <-time.After(time.Second):
			t.Fatal("timeout waiting for message")

		case err := <-errs:
			t.Fatal(err)
		}
	}

	// ensure all expected messages were received
MATCH:
	for _, m := range sent {
		expected := string(m.Payload)
		for _, g := range got {
			if g == expected {
				continue MATCH
			}
		}
		t.Fatalf("expected message %q", expected)
	}

	// ensure the managed consumer sent FLOW commands
	expectedFrames = make([]api.BaseCommand_Type, len(sent)/queueSize)
	for i := range expectedFrames {
		expectedFrames[i] = api.BaseCommand_FLOW
	}
	if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}
	t.Logf("received %d FLOW messages", len(expectedFrames))
}

func TestManagedConsumer_SrvClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cp := NewManagedClientPool()
	NewManagedConsumer(cp, ManagedConsumerConfig{
		ManagedClientConfig: ManagedClientConfig{
			ClientConfig: ClientConfig{
				Addr: srv.Addr,
			},
		},
		NewConsumerTimeout: time.Second,
		Topic:              "test-topic",
		Name:               "test",
		Exclusive:          false,
	})

	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
		api.BaseCommand_LOOKUP,
		api.BaseCommand_SUBSCRIBE,
	}
	for i := 0; i < 3; i++ {
		if err := srv.CloseAll(); err != nil {
			t.Fatal(err)
		}
		if err := srv.AssertReceived(ctx, expectedFrames...); err != nil {
			t.Fatal(err)
		}
	}
}

func TestManagedConsumer_ConsumerClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cp := NewManagedClientPool()
	NewManagedConsumer(cp, ManagedConsumerConfig{
		ManagedClientConfig: ManagedClientConfig{
			ClientConfig: ClientConfig{
				Addr: srv.Addr,
			},
		},
		NewConsumerTimeout: time.Second,
		Topic:              "test-topic",
		Name:               "test",
		Exclusive:          false,
	})

	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
	}
	if err := srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		expectedFrames := []api.BaseCommand_Type{
			api.BaseCommand_LOOKUP,
		}
		if err := srv.AssertReceived(ctx, expectedFrames...); err != nil {
			t.Fatal(err)
		}

		// Wait for the SUBSCRIBE message, and fetch the
		// consumerID from it. Then, use it to send the CLOSE_CONSUMER
		// message

		var consumerID uint64
		select {
		case f := <-srv.Received:
			if got, expected := f.BaseCmd.GetType(), api.BaseCommand_SUBSCRIBE; got != expected {
				t.Fatalf("got frame type %q; expected %q", got, expected)
			}
			consumerID = f.BaseCmd.GetSubscribe().GetConsumerId()

		case <-time.After(time.Second):
			t.Fatal("timeout waiting for SUBSCRIBE message")
		}

		// This will be sent to the client, closing the Consumer.
		closeConsumer := frame.Frame{
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_CLOSE_CONSUMER.Enum(),
				CloseConsumer: &api.CommandCloseConsumer{
					ConsumerId: proto.Uint64(consumerID),
					RequestId:  proto.Uint64(42),
				},
			},
		}
		if err := srv.Broadcast(closeConsumer); err != nil {
			t.Fatal(err)
		}
	}
}
