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

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/golang/protobuf/proto"
)

// newPubsub returns a ready-to-use pubsub.
func newPubsub(s cmdSender, dispatcher *frameDispatcher, subscriptions *subscriptions, reqID *monotonicID) *pubsub {
	return &pubsub{
		s:             s,
		reqID:         reqID,
		producerID:    &monotonicID{0},
		consumerID:    &monotonicID{0},
		dispatcher:    dispatcher,
		subscriptions: subscriptions,
	}
}

// pubsub is responsible for creating producers and consumers on a give topic.
type pubsub struct {
	s          cmdSender
	reqID      *monotonicID
	producerID *monotonicID
	consumerID *monotonicID

	dispatcher    *frameDispatcher // handles request response state
	subscriptions *subscriptions
}

// subscribe subscribes to the given topic. The queueSize determines the buffer
// size of the Consumer.Messages() channel.
func (t *pubsub) subscribe(ctx context.Context, topic, sub string, subType api.CommandSubscribe_SubType, queueSize int) (*Consumer, error) {
	requestID := t.reqID.next()
	consumerID := t.consumerID.next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_SUBSCRIBE.Enum(),
		Subscribe: &api.CommandSubscribe{
			SubType:      subType.Enum(),
			Topic:        proto.String(topic),
			Subscription: proto.String(sub),
			RequestId:    requestID,
			ConsumerId:   consumerID,
		},
	}

	resp, cancel, err := t.dispatcher.registerReqID(*requestID)
	if err != nil {
		return nil, err
	}
	defer cancel()

	c := newConsumer(t.s, t.dispatcher, topic, t.reqID, *consumerID, queueSize)
	// the new subscription needs to be added to the map
	// before sending the subscribe command, otherwise there'd
	// be a race between receiving the success result and
	// a possible message to the subscription
	t.subscriptions.addConsumer(c)

	if err := t.s.sendSimpleCmd(cmd); err != nil {
		t.subscriptions.delConsumer(c)
		return nil, err
	}

	// wait for a response or timeout

	select {
	case <-ctx.Done():
		t.subscriptions.delConsumer(c)
		return nil, ctx.Err()

	case f := <-resp:
		msgType := f.BaseCmd.GetType()
		// Possible responses types are:
		//  - Success (why not SubscribeSuccess?)
		//  - Error
		switch msgType {
		case api.BaseCommand_SUCCESS:
			return c, nil

		case api.BaseCommand_ERROR:
			t.subscriptions.delConsumer(c)

			errMsg := f.BaseCmd.GetError()
			return nil, fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())

		default:
			t.subscriptions.delConsumer(c)

			return nil, newErrUnexpectedMsg(msgType, *requestID)
		}
	}
}

// producer creates a new producer for the given topic and producerName.
func (t *pubsub) producer(ctx context.Context, topic, producerName string) (*Producer, error) {
	requestID := t.reqID.next()
	producerID := t.producerID.next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_PRODUCER.Enum(),
		Producer: &api.CommandProducer{
			RequestId:  requestID,
			ProducerId: producerID,
			Topic:      proto.String(topic),
		},
	}
	if producerName != "" {
		cmd.Producer.ProducerName = proto.String(producerName)
	}

	resp, cancel, err := t.dispatcher.registerReqID(*requestID)
	if err != nil {
		return nil, err
	}
	defer cancel()

	p := newProducer(t.s, t.dispatcher, t.reqID, *producerID)
	// the new producer needs to be added to subscriptions before sending
	// the create command to avoid potential race conditions
	t.subscriptions.addProducer(p)

	if err := t.s.sendSimpleCmd(cmd); err != nil {
		t.subscriptions.delProducer(p)
		return nil, err
	}

	// wait for the success response, error, or timeout

	select {
	case <-ctx.Done():
		t.subscriptions.delProducer(p)
		return nil, ctx.Err()

	case f := <-resp:
		msgType := f.BaseCmd.GetType()
		// Possible responses types are:
		//  - ProducerSuccess
		//  - Error
		switch msgType {
		case api.BaseCommand_PRODUCER_SUCCESS:
			success := f.BaseCmd.GetProducerSuccess()
			// TODO: is this a race?
			p.producerName = success.GetProducerName()
			return p, nil

		case api.BaseCommand_ERROR:
			t.subscriptions.delProducer(p)

			errMsg := f.BaseCmd.GetError()
			return nil, fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())

		default:
			t.subscriptions.delProducer(p)

			return nil, newErrUnexpectedMsg(msgType, *requestID)
		}
	}
}
