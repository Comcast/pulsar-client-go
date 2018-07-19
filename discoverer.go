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

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/golang/protobuf/proto"
)

// newDiscoverer returns a ready-to-use discoverer
func newDiscoverer(s cmdSender, dispatcher *frameDispatcher, reqID *monotonicID) *discoverer {
	return &discoverer{
		s:          s,
		reqID:      reqID,
		dispatcher: dispatcher,
	}
}

// discoverer is responsible for topic discovery and metadata lookups.
//
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Servicediscovery-40v5m
type discoverer struct {
	s          cmdSender
	reqID      *monotonicID
	dispatcher *frameDispatcher
}

// partitionedMetadata performs a PARTITIONED_METADATA request for the given
// topic. The response can be used to determine how many, if any, partitions
// there are for the topic.
//
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Partitionedtopicsdiscovery-g14a9h
func (d *discoverer) partitionedMetadata(ctx context.Context, topic string) (*api.CommandPartitionedTopicMetadataResponse, error) {
	requestID := d.reqID.next()
	cmd := api.BaseCommand{
		Type: api.BaseCommand_PARTITIONED_METADATA.Enum(),
		PartitionMetadata: &api.CommandPartitionedTopicMetadata{
			RequestId: requestID,
			Topic:     proto.String(topic),
		},
	}

	resp, cancel, err := d.dispatcher.registerReqID(*requestID)
	if err != nil {
		return nil, err
	}
	defer cancel()

	if err := d.s.sendSimpleCmd(cmd); err != nil {
		return nil, err
	}

	// wait for response or timeout

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case f := <-resp:
		return f.BaseCmd.GetPartitionMetadataResponse(), nil
	}
}

// lookupTopic performs a LOOKUP request for the given topic. The response
// will determine the proper broker to use for the topic, or indicate
// that another LOOKUP request is necessary.
//
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Topiclookup-dk72wp
func (d *discoverer) lookupTopic(ctx context.Context, topic string, authoritative bool) (*api.CommandLookupTopicResponse, error) {
	requestID := d.reqID.next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_LOOKUP.Enum(),
		LookupTopic: &api.CommandLookupTopic{
			RequestId:     requestID,
			Topic:         proto.String(topic),
			Authoritative: proto.Bool(authoritative),
		},
	}

	resp, cancel, err := d.dispatcher.registerReqID(*requestID)
	if err != nil {
		return nil, err
	}
	defer cancel()

	if err := d.s.sendSimpleCmd(cmd); err != nil {
		return nil, err
	}

	// wait for async response or timeout

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case f := <-resp:
		return f.BaseCmd.GetLookupTopicResponse(), nil
	}
}
