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
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/Comcast/pulsar-client-go/api"
)

func TestConsumer_Flow(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	consID := uint64(123)
	reqID := monotonicID{id}
	dispatcher := newFrameDispatcher()

	c := newConsumer(&ms, dispatcher, "test", &reqID, consID, 1)

	if err := c.Flow(123); err != nil {
		t.Fatalf("Flow() err = %v; nil expected", err)
	}

	if got, expected := len(ms.frames), 1; got != expected {
		t.Fatalf("got %d frame; expected %d", got, expected)
	}
}

func TestConsumer_Close_Success(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	consID := uint64(123)
	reqID := monotonicID{id}
	dispatcher := newFrameDispatcher()

	c := newConsumer(&ms, dispatcher, "test", &reqID, consID, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := make(chan error, 1)

	go func() { resp <- c.Close(ctx) }()

	// Allow goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	select {
	case <-c.Closed():
		t.Fatalf("Closed() unblocked; expected to be blocked before receiving Close() response")
	default:
		t.Logf("Closed() blocked")
	}

	expected := api.CommandSuccess{
		RequestId: proto.Uint64(id),
	}
	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type:    api.BaseCommand_SUCCESS.Enum(),
			Success: &expected,
		},
	}
	if err := dispatcher.notifyReqID(id, f); err != nil {
		t.Fatal(err)
	}

	got := <-resp
	if got != nil {
		t.Fatalf("Close() err = %v; nil expected", got)
	}

	if got, expected := len(ms.frames), 1; got != expected {
		t.Fatalf("got %d frame; expected %d", got, expected)
	}

	select {
	case <-c.Closed():
		t.Logf("Closed() unblocked")
	default:
		t.Fatalf("Closed() blocked; expected to be unblocked after Close()")
	}
}

func TestConsumer_handleMessage(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	consID := uint64(123)
	reqID := monotonicID{id}
	dispatcher := newFrameDispatcher()

	c := newConsumer(&ms, dispatcher, "test", &reqID, consID, 1)

	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_MESSAGE.Enum(),
			Message: &api.CommandMessage{
				ConsumerId: proto.Uint64(consID),
			},
		},
		Metadata: &api.MessageMetadata{
			ProducerName: proto.String("hi"),
			SequenceId:   proto.Uint64(9933),
		},
		Payload: []byte("hola mundo"),
	}

	resp := make(chan error, 1)

	go func() {
		resp <- c.handleMessage(f)
	}()

	var got Message
	select {
	case got = <-c.Messages():
	case <-time.After(time.Millisecond * 250):
		t.Fatal("timeout waiting for message")
	}

	if !proto.Equal(got.Msg, f.BaseCmd.Message) {
		t.Fatalf("got message:\n%+v\nexpected:\n%+v", got.Msg, f.BaseCmd.Message)
	}
	if !proto.Equal(got.Meta, f.Metadata) {
		t.Fatalf("got meta:\n%+v\nexpected:\n%+v", got.Meta, f.Metadata)
	}
	if !bytes.Equal(got.Payload, f.Payload) {
		t.Fatalf("got payload:\n%q\nexpected:\n%q", got.Payload, f.Payload)
	}

	t.Logf("got message:\n%+v", got)
}

func TestConsumer_handleMessage_fullQueue(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	consID := uint64(123)
	reqID := monotonicID{id}
	dispatcher := newFrameDispatcher()

	queueSize := 3
	c := newConsumer(&ms, dispatcher, "test", &reqID, consID, queueSize)

	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_MESSAGE.Enum(),
			Message: &api.CommandMessage{
				ConsumerId: proto.Uint64(consID),
			},
		},
		Metadata: &api.MessageMetadata{
			ProducerName: proto.String("hi"),
			SequenceId:   proto.Uint64(9933),
		},
		Payload: []byte("hola mundo"),
	}

	for i := 0; i < queueSize; i++ {
		if err := c.handleMessage(f); err != nil {
			t.Fatalf("handleMessage() err = %v; expected nil for msg number %d and queueSize %d", err, i+1, queueSize)
		}

		if got, expected := len(c.overflow), 0; got != expected {
			t.Fatalf("len(consumer overflow buffer) = %d; expected %d", got, expected)
		}
	}

	// message queue should now be full,
	// therefore handleMessage should return an error
	err := c.handleMessage(f)
	if err == nil {
		t.Fatalf("handleMessage() err = %v expected non-nil for msg number %d and queueSize %d", err, queueSize+1, queueSize)
	}
	t.Logf("handleMessage() err (expected) = %q for msg number %d and queueSize %d", err, queueSize+1, queueSize)

	if got, expected := len(c.overflow), 1; got != expected {
		t.Fatalf("len(consumer overflow buffer) = %d; expected %d", got, expected)
	}

	for i := 0; i < queueSize; i++ {
		select {
		case got := <-c.Messages():
			if !proto.Equal(got.Msg, f.BaseCmd.Message) {
				t.Fatalf("got message:\n%+v\nexpected:\n%+v", got.Msg, f.BaseCmd.Message)
			}
			if !proto.Equal(got.Meta, f.Metadata) {
				t.Fatalf("got meta:\n%+v\nexpected:\n%+v", got.Meta, f.Metadata)
			}
			if !bytes.Equal(got.Payload, f.Payload) {
				t.Fatalf("got payload:\n%q\nexpected:\n%q", got.Payload, f.Payload)
			}
			t.Logf("got message:\n%+v", got)
		case <-time.After(time.Millisecond * 250):
			t.Fatal("timeout waiting for message")
		}
	}
}

func TestConsumer_handleCloseConsumer(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	consID := uint64(123)
	reqID := monotonicID{id}
	dispatcher := newFrameDispatcher()

	c := newConsumer(&ms, dispatcher, "test", &reqID, consID, 1)

	select {
	case <-c.Closed():
		t.Fatalf("Closed() unblocked; expected to be blocked before receiving handleCloseConsumer()")
	default:
		t.Logf("Closed() blocked")
	}

	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_CLOSE_CONSUMER.Enum(),
			CloseConsumer: &api.CommandCloseConsumer{
				RequestId:  proto.Uint64(id),
				ConsumerId: proto.Uint64(consID),
			},
		},
	}
	if err := c.handleCloseConsumer(f); err != nil {
		t.Fatalf("handleCloseConsumer() err = %v; nil expected", err)
	}

	select {
	case <-c.Closed():
		t.Logf("Closed() unblocked")
	default:
		t.Fatalf("Closed() blocked; expected to be unblocked after handleCloseConsumer()")
	}
}

func TestConsumer_handleReachedEndOfTopic(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	consID := uint64(123)
	reqID := monotonicID{id}
	dispatcher := newFrameDispatcher()

	c := newConsumer(&ms, dispatcher, "test", &reqID, consID, 1)

	select {
	case <-c.ReachedEndOfTopic():
		t.Fatalf("ReachedEndOfTopic() unblocked; expected to be blocked before receiving handleReachedEndOfTopic()")
	default:
		t.Logf("ReachedEndOfTopic() blocked")
	}

	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_REACHED_END_OF_TOPIC.Enum(),
			ReachedEndOfTopic: &api.CommandReachedEndOfTopic{
				ConsumerId: proto.Uint64(consID),
			},
		},
	}
	if err := c.handleReachedEndOfTopic(f); err != nil {
		t.Fatalf("handleReachedEndOfTopic() err = %v; nil expected", err)
	}

	select {
	case <-c.ReachedEndOfTopic():
		t.Logf("ReachedEndOfTopic() unblocked")
	default:
		t.Fatalf("ReachedEndOfTopic() blocked; expected to be unblocked after handleReachedEndOfTopic()")
	}
}

func TestConsumer_RedeliverOverflow(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	consID := uint64(123)
	reqID := monotonicID{id}
	dispatcher := newFrameDispatcher()

	queueSize := 1
	N := 8 // number of messages to push to consumer
	c := newConsumer(&ms, dispatcher, "test", &reqID, consID, queueSize)

	for i := 0; i < N; i++ {
		entryID := uint64(i)
		// the MessageIdData must be unique for each message,
		// otherwise the consumer will consider them duplicates
		// and not store them in overflow
		f := Frame{
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_MESSAGE.Enum(),
				Message: &api.CommandMessage{
					ConsumerId: proto.Uint64(consID),
					MessageId: &api.MessageIdData{
						EntryId: &entryID,
					},
				},
			},
			Metadata: &api.MessageMetadata{
				ProducerName: proto.String("hi"),
				SequenceId:   proto.Uint64(9933),
			},
			Payload: []byte(fmt.Sprintf("%d: Hola", i)),
		}

		err := c.handleMessage(f)
		if i < queueSize {
			if err != nil {
				t.Fatalf("handleMessage() err = %v; expected nil for msg number %d and queueSize %d", err, i, queueSize)
			}
		} else {
			if err == nil {
				t.Fatalf("handleMessage() err = %v; expected non-nil for msg number %d and queueSize %d", err, i, queueSize)
			}
		}
	}

	if got, err := c.RedeliverOverflow(context.Background()); err != nil {
		t.Fatalf("RedeliverOverflow() err = %v; expected nil", err)
	} else if expected := N - queueSize; got != expected {
		t.Fatalf("RedeliverOverflow() = %d; expected %d", got, expected)
	}

	// Ensure correct frame was sent by consumer

	sentFrames := ms.getFrames()
	if got, expected := len(sentFrames), 1; got != expected {
		t.Fatalf("%d frames were sent; expected %d", got, expected)
	}

	f := sentFrames[0]

	if got, expected := f.BaseCmd.GetType(), api.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES; got != expected {
		t.Fatalf("got frame of type %q; expected %q", got, expected)
	}

	// Ensure messageIDs are correct

	messageIDs := f.BaseCmd.GetRedeliverUnacknowledgedMessages().GetMessageIds()

	if got, expected := len(messageIDs), N-queueSize; got != expected {
		t.Fatalf("REDELIVER_UNACKNOWLEDGED_MESSAGES message contained %d messageIDs; expected %d", got, expected)
	}

	for i, mid := range messageIDs {
		if got, expected := mid.GetEntryId(), uint64(i+1); got != expected {
			t.Fatalf("messageID %d: EntryID = %d; expected %d", i, got, expected)
		}
	}
}
