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
	"crypto/tls"
	"fmt"
	"time"

	"github.com/Comcast/pulsar-client-go/api"
)

const (
	// ProtoVersion is the Pulsar protocol version
	// used by this client.
	ProtoVersion = int32(api.ProtocolVersion_v12)
	// ClientVersion is an opaque string sent
	// by the client to the server on connect, eg:
	// "Pulsar-Client-Java-v1.15.2"
	ClientVersion = "pulsar-client-go"
	// authMethodTLS is the name of the TLS authentication
	// method, used in the CONNECT message.
	authMethodTLS = "tls"
)

// undefRequestID defines a RequestID of -1.
//
// Usage example:
// https://github.com/apache/incubator-pulsar/blob/fdc7b8426d8253c9437777ae51a4639239550f00/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/ServerCnx.java#L325
const undefRequestID = 1<<64 - 1

// ClientConfig is used to configure a Pulsar client.
type ClientConfig struct {
	Addr        string        // pulsar broker address. May start with pulsar://
	phyAddr     string        // if set, the TCP connection should be made using this address. This is only ever set during Topic Lookup
	DialTimeout time.Duration // timeout to use when establishing TCP connection
	TLSConfig   *tls.Config   // TLS configuration. May be nil, in which case TLS will not be used
	Errs        chan<- error  // asynchronous errors will be sent here. May be nil
}

// connAddr returns the address that should be used
// for the TCP connection. It defaults to phyAddr if set,
// otherwise Addr. This is to support the proxying through
// a broker, as determined during topic lookup.
func (c ClientConfig) connAddr() string {
	if c.phyAddr != "" {
		return c.phyAddr
	}
	return c.Addr
}

// setDefaults returns a modified config with appropriate zero values set to defaults.
func (c ClientConfig) setDefaults() ClientConfig {
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}

	return c
}

// NewClient returns a Pulsar client for the given configuration options.
func NewClient(cfg ClientConfig) (*Client, error) {
	cfg = cfg.setDefaults()

	var cnx *conn
	var err error

	if cfg.TLSConfig != nil {
		cnx, err = newTLSConn(cfg.connAddr(), cfg.TLSConfig, cfg.DialTimeout)
	} else {
		cnx, err = newTCPConn(cfg.connAddr(), cfg.DialTimeout)
	}
	if err != nil {
		return nil, err
	}

	reqID := monotonicID{0}

	dispatcher := newFrameDispatcher()
	subs := newSubscriptions()

	c := &Client{
		c:         cnx,
		asyncErrs: asyncErrors(cfg.Errs),

		dispatcher:    dispatcher,
		subscriptions: subs,
		connector:     newConnector(cnx, dispatcher),
		pinger:        newPinger(cnx, dispatcher),
		discoverer:    newDiscoverer(cnx, dispatcher, &reqID),
		pubsub:        newPubsub(cnx, dispatcher, subs, &reqID),
	}

	handler := func(f Frame) {
		// All message types can be handled in
		// parallel, since their ordering should not matter
		go c.handleFrame(f)
	}

	go func() {
		// If conn.read() unblocks, it indicates that
		// the connection has been closed and is no longer usable.
		defer func() {
			if err := c.Close(); err != nil {
				c.asyncErrs.send(err)
			}
		}()

		if err := cnx.read(handler); err != nil {
			c.asyncErrs.send(err)
		}
	}()

	return c, nil
}

// cmdSender is an interface that is capable of sending
// commands to Pulsar. It allows abstraction of a conn.
type cmdSender interface {
	sendSimpleCmd(cmd api.BaseCommand) error
	sendPayloadCmd(cmd api.BaseCommand, metadata api.MessageMetadata, payload []byte) error
	closed() <-chan struct{} // closed unblocks when the connection has been closed
}

// Client is a Pulsar client, capable of sending and receiving
// messages and managing the associated state.
type Client struct {
	c         *conn
	asyncErrs asyncErrors

	dispatcher *frameDispatcher

	subscriptions *subscriptions
	connector     *connector
	pinger        *pinger
	discoverer    *discoverer
	pubsub        *pubsub
}

// Closed returns a channel that unblocks when the client's connection
// has been closed and is no longer usable. Users should monitor this
// channel and recreate the Client if closed.
// TODO: Rename to Done
func (c *Client) Closed() <-chan struct{} {
	return c.c.closed()
}

// Close closes the connection. The channel returned from `Closed` will unblock.
// The client should no longer be used after calling Close.
func (c *Client) Close() error {
	return c.c.close()
}

// Connect sends a Connect message to the Pulsar server, then
// waits for either a CONNECTED response or the context to
// timeout. Connect should be called immediately after
// creating a client, before sending any other messages.
// The "auth method" is not set in the CONNECT message.
// See ConnectTLS for TLS auth method.
// The proxyBrokerURL may be blank, or it can be used to indicate
// that the client is connecting through a proxy server.
// See "Connection establishment" for more info:
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Connectionestablishment-6pslvw
func (c *Client) Connect(ctx context.Context, proxyBrokerURL string) (*api.CommandConnected, error) {
	return c.connector.connect(ctx, "", proxyBrokerURL)
}

// ConnectTLS sends a Connect message to the Pulsar server, then
// waits for either a CONNECTED response or the context to
// timeout. Connect should be called immediately after
// creating a client, before sending any other messages.
// The "auth method" is set to tls in the CONNECT message.
// The proxyBrokerURL may be blank, or it can be used to indicate
// that the client is connecting through a proxy server.
// See "Connection establishment" for more info:
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Connectionestablishment-6pslvw
func (c *Client) ConnectTLS(ctx context.Context, proxyBrokerURL string) (*api.CommandConnected, error) {
	return c.connector.connect(ctx, authMethodTLS, proxyBrokerURL)
}

// Ping sends a PING message to the Pulsar server, then
// waits for either a PONG response or the context to
// timeout.
func (c *Client) Ping(ctx context.Context) error {
	return c.pinger.ping(ctx)
}

// LookupTopic returns metadata about the given topic. Topic lookup needs
// to be performed each time a client needs to create or reconnect a
// producer or a consumer. Lookup is used to discover which particular
// broker is serving the topic we are about to use.
//
// The command has to be used in a connection that has already gone
// through the Connect / Connected initial handshake.
// See "Topic lookup" for more info:
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Topiclookup-rxds6i
func (c *Client) LookupTopic(ctx context.Context, topic string, authoritative bool) (*api.CommandLookupTopicResponse, error) {
	return c.discoverer.lookupTopic(ctx, topic, authoritative)
}

// NewProducer creates a new producer capable of sending message to the
// given topic.
func (c *Client) NewProducer(ctx context.Context, topic, producerName string) (*Producer, error) {
	return c.pubsub.producer(ctx, topic, producerName)
}

// NewSharedConsumer creates a new shared consumer capable of reading messages from the
// given topic.
// See "Subscription modes" for more information:
// https://pulsar.incubator.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Subscriptionmodes-jdrefl
func (c *Client) NewSharedConsumer(ctx context.Context, topic, subscriptionName string, queue chan Message) (*Consumer, error) {
	return c.pubsub.subscribe(ctx, topic, subscriptionName, api.CommandSubscribe_Shared, queue)
}

// NewExclusiveConsumer creates a new exclusive consumer capable of reading messages from the
// given topic.
// See "Subscription modes" for more information:
// https://pulsar.incubator.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Subscriptionmodes-jdrefl
func (c *Client) NewExclusiveConsumer(ctx context.Context, topic, subscriptionName string, queue chan Message) (*Consumer, error) {
	return c.pubsub.subscribe(ctx, topic, subscriptionName, api.CommandSubscribe_Exclusive, queue)
}

// NewFailoverConsumer creates a new failover consumer capable of reading messages from the
// given topic.
// See "Subscription modes" for more information:
// https://pulsar.incubator.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Subscriptionmodes-jdrefl
func (c *Client) NewFailoverConsumer(ctx context.Context, topic, subscriptionName string, queue chan Message) (*Consumer, error) {
	return c.pubsub.subscribe(ctx, topic, subscriptionName, api.CommandSubscribe_Failover, queue)
}

// handleFrame is called by the underlaying conn with
// all received Frames.
func (c *Client) handleFrame(f Frame) {
	var err error

	msgType := f.BaseCmd.GetType()

	switch msgType {

	// Solicited responses with NO response ID associated

	case api.BaseCommand_CONNECTED:
		err = c.dispatcher.notifyGlobal(f)

	case api.BaseCommand_PONG:
		err = c.dispatcher.notifyGlobal(f)

	// Solicited responses with a requestID to correlate
	// it to its request

	case api.BaseCommand_SUCCESS:
		err = c.dispatcher.notifyReqID(f.BaseCmd.GetSuccess().GetRequestId(), f)

	case api.BaseCommand_ERROR:
		err = c.dispatcher.notifyReqID(f.BaseCmd.GetError().GetRequestId(), f)

	case api.BaseCommand_LOOKUP_RESPONSE:
		err = c.dispatcher.notifyReqID(f.BaseCmd.GetLookupTopicResponse().GetRequestId(), f)

	case api.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		err = c.dispatcher.notifyReqID(f.BaseCmd.GetPartitionMetadataResponse().GetRequestId(), f)

	case api.BaseCommand_PRODUCER_SUCCESS:
		err = c.dispatcher.notifyReqID(f.BaseCmd.GetProducerSuccess().GetRequestId(), f)

	// Solicited responses with a (producerID, sequenceID) tuple to correlate
	// it to its request

	case api.BaseCommand_SEND_RECEIPT:
		msg := f.BaseCmd.GetSendReceipt()
		err = c.dispatcher.notifyProdSeqIDs(msg.GetProducerId(), msg.GetSequenceId(), f)

	case api.BaseCommand_SEND_ERROR:
		msg := f.BaseCmd.GetSendError()
		err = c.dispatcher.notifyProdSeqIDs(msg.GetProducerId(), msg.GetSequenceId(), f)

	// Unsolicited responses that have a producer ID

	case api.BaseCommand_CLOSE_PRODUCER:
		err = c.subscriptions.handleCloseProducer(f.BaseCmd.GetCloseProducer().GetProducerId(), f)

	// Unsolicited responses that have a consumer ID

	case api.BaseCommand_CLOSE_CONSUMER:
		err = c.subscriptions.handleCloseConsumer(f.BaseCmd.GetCloseConsumer().GetConsumerId(), f)

	case api.BaseCommand_REACHED_END_OF_TOPIC:
		err = c.subscriptions.handleReachedEndOfTopic(f.BaseCmd.GetReachedEndOfTopic().GetConsumerId(), f)

	case api.BaseCommand_MESSAGE:
		err = c.subscriptions.handleMessage(f.BaseCmd.GetMessage().GetConsumerId(), f)

	// Unsolicited responses

	case api.BaseCommand_PING:
		err = c.pinger.handlePing(msgType, f.BaseCmd.GetPing())

	default:
		err = fmt.Errorf("unhandled message of type %q", f.BaseCmd.GetType())
	}

	if err != nil {
		c.asyncErrs.send(err)
	}
}
