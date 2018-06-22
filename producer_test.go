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
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/Comcast/pulsar-client-go/api"
)

func TestProducer_Send_Success(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	prodID := uint64(123)
	reqID := monotonicID{id}
	dispatcher := newFrameDispatcher()
	payload := []byte("hola mundo")

	p := newProducer(&ms, dispatcher, &reqID, prodID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type response struct {
		success *api.CommandSendReceipt
		err     error
	}
	resp := make(chan response, 1)

	go func() {
		var r response
		r.success, r.err = p.Send(ctx, payload)
		resp <- r
	}()

	// Allow goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	expected := api.CommandSendReceipt{
		ProducerId: proto.Uint64(prodID),
		SequenceId: proto.Uint64(0),
	}
	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type:        api.BaseCommand_SEND_RECEIPT.Enum(),
			SendReceipt: &expected,
		},
	}
	if err := dispatcher.notifyProdSeqIDs(prodID, 0, f); err != nil {
		t.Fatal(err)
	}

	r := <-resp
	if r.err != nil {
		t.Fatalf("Send() err = %v; nil expected", r.err)
	}

	if got := r.success; !proto.Equal(got, &expected) {
		t.Fatalf("Send() got:\n%+v\nexpected:\n%+v", got, &expected)
	}

	if got, expected := len(ms.frames), 1; got != expected {
		t.Fatalf("got %d frame; expected %d", got, expected)
	}
}

func TestProducer_Send_Error(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	prodID := uint64(123)
	reqID := monotonicID{id}
	dispatcher := newFrameDispatcher()
	payload := []byte("hola mundo")

	p := newProducer(&ms, dispatcher, &reqID, prodID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type response struct {
		success *api.CommandSendReceipt
		err     error
	}
	resp := make(chan response, 1)

	go func() {
		var r response
		r.success, r.err = p.Send(ctx, payload)
		resp <- r
	}()

	// Allow goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_SEND_ERROR.Enum(),
			SendError: &api.CommandSendError{
				Message:    proto.String("no me mandes esto"),
				ProducerId: proto.Uint64(prodID),
				SequenceId: proto.Uint64(0),
			},
		},
	}
	if err := dispatcher.notifyProdSeqIDs(prodID, 0, f); err != nil {
		t.Fatal(err)
	}

	r := <-resp
	if r.err == nil {
		t.Fatalf("Send() err = %v; non-nil expected", r.err)
	}
	t.Logf("Send() err = %v", r.err)

	if got, expected := len(ms.frames), 1; got != expected {
		t.Fatalf("got %d frame; expected %d", got, expected)
	}
}

func TestProducer_Close_Success(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	prodID := uint64(123)
	reqID := monotonicID{id}
	dispatcher := newFrameDispatcher()

	p := newProducer(&ms, dispatcher, &reqID, prodID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := make(chan error, 1)

	go func() { resp <- p.Close(ctx) }()

	// Allow goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	select {
	case <-p.Closed():
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
	case <-p.Closed():
		t.Logf("Closed() unblocked")
	default:
		t.Fatalf("Closed() blocked; expected to be unblocked after Close()")
	}
}

func TestProducer_handleCloseProducer(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	prodID := uint64(123)
	reqID := monotonicID{id}
	dispatcher := newFrameDispatcher()

	p := newProducer(&ms, dispatcher, &reqID, prodID)

	select {
	case <-p.Closed():
		t.Fatalf("Closed() unblocked; expected to be blocked before receiving handleCloseProducer()")
	default:
		t.Logf("Closed() blocked")
	}

	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_CLOSE_PRODUCER.Enum(),
			CloseProducer: &api.CommandCloseProducer{
				RequestId:  proto.Uint64(id),
				ProducerId: proto.Uint64(prodID),
			},
		},
	}
	if err := p.handleCloseProducer(f); err != nil {
		t.Fatalf("handleCloseProducer() err = %v; expected nil", err)
	}

	select {
	case <-p.Closed():
		t.Logf("Closed() unblocked")
	default:
		t.Fatalf("Closed() blocked; expected to be unblocked after handleCloseProducer()")
	}
}
