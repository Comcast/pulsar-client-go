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

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/golang/protobuf/proto"
)

func TestDiscoverer_PartitionedMetadata(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	reqID := monotonicID{id}

	dispatcher := newFrameDispatcher()
	d := newDiscoverer(&ms, dispatcher, &reqID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type response struct {
		success *api.CommandPartitionedTopicMetadataResponse
		err     error
	}
	resp := make(chan response, 1)

	go func() {
		var r response
		r.success, r.err = d.partitionedMetadata(ctx, "test")
		resp <- r
	}()

	// Allow goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	expected := api.CommandPartitionedTopicMetadataResponse{
		RequestId: proto.Uint64(id),
		Message:   proto.String("hi"),
	}
	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_PARTITIONED_METADATA_RESPONSE.Enum(),
			PartitionMetadataResponse: &expected,
		},
	}
	if err := dispatcher.notifyReqID(id, f); err != nil {
		t.Fatalf("HandleReqID() err = %v; nil expected", err)
	}

	r := <-resp

	if r.err != nil {
		t.Fatalf("discoverer.partitionedMetadata() err = %v; nil expected", r.err)
	}

	if !proto.Equal(r.success, &expected) {
		t.Fatalf("discoverer.partionedMetadata() response = %v; expected %v", r.success, expected)
	}
}

func TestDiscoverer_LookupTopic(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	reqID := monotonicID{id}

	dispatcher := newFrameDispatcher()
	d := newDiscoverer(&ms, dispatcher, &reqID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type response struct {
		success *api.CommandLookupTopicResponse
		err     error
	}
	resp := make(chan response, 1)

	go func() {
		var r response
		r.success, r.err = d.lookupTopic(ctx, "test", false)
		resp <- r
	}()

	// Allow goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	expected := api.CommandLookupTopicResponse{
		RequestId: proto.Uint64(id),
		Message:   proto.String("hi"),
	}
	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type:                api.BaseCommand_LOOKUP_RESPONSE.Enum(),
			LookupTopicResponse: &expected,
		},
	}
	if err := dispatcher.notifyReqID(id, f); err != nil {
		t.Fatalf("HandleReqID() err = %v; nil expected", err)
	}

	r := <-resp
	if r.err != nil {
		t.Fatalf("discoverer.lookupTopic() err = %v; nil expected", r.err)
	}

	if !proto.Equal(r.success, &expected) {
		t.Fatalf("discoverer.lookupTopic() response = %v; expected %v", r.success, expected)
	}
}

func TestDiscoverer_LookupTopic_BadRequestID(t *testing.T) {
	var ms mockSender
	id := uint64(43)
	reqID := monotonicID{id}

	dispatcher := newFrameDispatcher()
	d := newDiscoverer(&ms, dispatcher, &reqID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type response struct {
		success *api.CommandLookupTopicResponse
		err     error
	}
	resp := make(chan response, 1)

	go func() {
		var r response
		r.success, r.err = d.lookupTopic(ctx, "test", false)
		resp <- r
	}()

	// Allow goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	expected := api.CommandLookupTopicResponse{
		RequestId: proto.Uint64(id + 1), // incorrect RequestID
		Message:   proto.String("hi"),
	}
	f := Frame{
		BaseCmd: &api.BaseCommand{
			Type:                api.BaseCommand_LOOKUP_RESPONSE.Enum(),
			LookupTopicResponse: &expected,
		},
	}
	// incorrect RequestID
	if err := dispatcher.notifyReqID(id+1, f); err == nil {
		t.Fatalf("HandleReqID() err = %v; non-nil expected", err)
	} else {
		t.Logf("HandleReqID() err = %v", err)
	}

	// cause lookupTopic to timeout
	cancel()

	r := <-resp

	if r.success != nil {
		t.Fatalf("discoverer.lookupTopic() got unexpected response = %v; expected nil", r.success)
	}

	if r.err == nil {
		t.Fatalf("discoverer.lookupTopic() err = %v; expected non-nil", r.err)
	}
	t.Logf("discoverer.lookupTopic() err = %v", r.err)
}
