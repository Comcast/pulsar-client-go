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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/Comcast/pulsar-client-go/frame"
	"github.com/golang/protobuf/proto"
)

// ErrClosedProducer is returned when attempting to send
// from a closed Producer.
var ErrClosedProducer = errors.New("producer is closed")

// newProducer returns a ready-to-use producer. A producer
// sends messages (type MESSAGE) to Pulsar.
func newProducer(s cmdSender, dispatcher *frameDispatcher, reqID *monotonicID, producerID uint64) *Producer {
	return &Producer{
		s:          s,
		producerID: producerID,
		reqID:      reqID,
		seqID:      &monotonicID{0},
		dispatcher: dispatcher,
		closedc:    make(chan struct{}),
	}
}

// Producer is responsible for creating a subscription producer and
// managing its state.
type Producer struct {
	s cmdSender

	producerID   uint64
	producerName string

	reqID *monotonicID
	seqID *monotonicID

	dispatcher *frameDispatcher // handles request/response state

	mu       sync.RWMutex // protects following
	isClosed bool
	closedc  chan struct{}
}

// Send sends a message and waits for a SendReceipt.
func (p *Producer) Send(ctx context.Context, payload []byte) (*api.CommandSendReceipt, error) {
	p.mu.RLock()
	if p.isClosed {
		p.mu.RUnlock()
		return nil, ErrClosedProducer
	}
	p.mu.RUnlock()

	sequenceID := p.seqID.next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_SEND.Enum(),
		Send: &api.CommandSend{
			ProducerId:  proto.Uint64(p.producerID),
			SequenceId:  sequenceID,
			NumMessages: proto.Int32(1),
		},
	}
	metadata := api.MessageMetadata{
		SequenceId:   sequenceID,
		ProducerName: proto.String(p.producerName),
		PublishTime:  proto.Uint64(uint64(time.Now().Unix()) * 1000),
		Compression:  api.CompressionType_NONE.Enum(),
	}

	resp, cancel, err := p.dispatcher.registerProdSeqIDs(p.producerID, *sequenceID)
	if err != nil {
		return nil, err
	}
	defer cancel()

	if err := p.s.sendPayloadCmd(cmd, metadata, payload); err != nil {
		return nil, err
	}

	// wait for timeout, closed producer, or response/error
	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case <-p.Closed():
		return nil, ErrClosedProducer

	case f := <-resp:
		msgType := f.BaseCmd.GetType()
		// Possible responses types are:
		//  - SendReceipt
		//  - SendError
		switch msgType {
		case api.BaseCommand_SEND_RECEIPT:
			return f.BaseCmd.GetSendReceipt(), nil

		case api.BaseCommand_SEND_ERROR:
			errMsg := f.BaseCmd.GetSendError()
			return nil, fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())

		default:
			return nil, newErrUnexpectedMsg(msgType, p.producerID, *sequenceID)
		}
	}
}

// Closed returns a channel that will block _unless_ the
// producer has been closed, in which case the channel will have
// been closed.
// TODO: Rename Done
func (p *Producer) Closed() <-chan struct{} {
	return p.closedc
}

// ConnClosed unblocks when the producer's connection has been closed. Once that
// happens, it's necessary to first recreate the client and then the producer.
func (p *Producer) ConnClosed() <-chan struct{} {
	return p.s.closed()
}

// Close closes the producer. When receiving a CloseProducer command,
// the broker will stop accepting any more messages for the producer,
// wait until all pending messages are persisted and then reply Success to the client.
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#command-closeproducer
func (p *Producer) Close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isClosed {
		return nil
	}

	requestID := p.reqID.next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_CLOSE_PRODUCER.Enum(),
		CloseProducer: &api.CommandCloseProducer{
			RequestId:  requestID,
			ProducerId: proto.Uint64(p.producerID),
		},
	}

	resp, cancel, err := p.dispatcher.registerReqID(*requestID)
	if err != nil {
		return err
	}
	defer cancel()

	if err := p.s.sendSimpleCmd(cmd); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-resp:
		p.isClosed = true
		close(p.closedc)

		return nil
	}
}

// handleCloseProducer should be called when a CLOSE_PRODUCER message is received
// associated with this producer.
// The broker can send a CloseProducer command to client when it’s performing a
// graceful failover (eg: broker is being restarted, or the topic is being unloaded
// by load balancer to be transferred to a different broker).
//
// When receiving the CloseProducer, the client is expected to go through the service discovery lookup again and recreate the producer again. The TCP connection is not being affected.
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#command-closeproducer
func (p *Producer) handleCloseProducer(f frame.Frame) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isClosed {
		return nil
	}

	p.isClosed = true
	close(p.closedc)

	return nil
}
