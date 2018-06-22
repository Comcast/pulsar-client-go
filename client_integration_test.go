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
	"io/ioutil"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Comcast/pulsar-client-go/api"
)

// TestClient_Int_PubSub creates a producer and multiple consumers.
// Messages are created by the producer, and then it is asserted
// that all the consumers receive those messages.
func TestClient_Int_PubSub(t *testing.T) {
	asyncErrs := make(chan error, 8)
	c, err := NewClient(ClientConfig{
		Addr: pulsarAddr(t),
		Errs: asyncErrs,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for err := range asyncErrs {
			t.Log(err)
		}
	}()

	// context for remainder of test
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	connected, err := c.Connect(ctx, "")
	if err != nil {
		t.Fatalf("client.Connect() err = %v", err)
	}
	t.Logf("CONNECTED: protocol=%d server=%q", connected.GetProtocolVersion(), connected.GetServerVersion())

	if err := c.Ping(ctx); err != nil {
		t.Fatalf("pinger.ping() err = %v", err)
	} else {
		t.Logf("PONG received")
	}

	topic := fmt.Sprintf("persistent://sample/standalone/ns1/test-%s", randString(32))
	t.Logf("test topic: %q", topic)

	topicResp, err := c.LookupTopic(ctx, topic, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, expected := topicResp.GetResponse(), api.CommandLookupTopicResponse_Connect; got != expected {
		t.Fatalf("topic lookup response = %q; expected %q", got, expected)
	}
	t.Log(topicResp.String())

	// number of messages to produce (and consume)
	N := 128
	payloads := make([]string, N)
	for i := 0; i < N; i++ {
		payloads[i] = fmt.Sprintf("%02d - Hola mundo, este es mensaje", i)
	}

	// create multiple consumers
	consumers := make([]*Consumer, 32)
	subName := randString(16)
	for i := range consumers {
		name := fmt.Sprintf("%s-%d", subName, i)
		cs, err := c.NewExclusiveConsumer(ctx, topic, name, N)
		if err != nil {
			t.Fatal(err)
		}
		// Notify pulsar server that it can send us N messages
		if err := cs.Flow(uint32(N)); err != nil {
			t.Fatal(err)
		}
		consumers[i] = cs
	}

	// create single producer
	topicProducer, err := c.NewProducer(ctx, topic, "test")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("write", func(t *testing.T) {
		// The "write" subtest cannot be run in parallel with
		// the readers/consumers, since it could get scheduled after a
		// "read" subtest finishes (timesout)
		for i := 0; i < N; i++ {
			if _, err := topicProducer.Send(ctx, []byte(payloads[i])); err != nil {
				t.Fatal(err)
			}
		}

		if err := topicProducer.Close(ctx); err != nil {
			t.Fatal(err)
		}

		select {
		case <-topicProducer.Closed():
		default:
			t.Fatal("Closed expected to be unblocked")
		}
	})

	t.Run("read-group", func(t *testing.T) {
		for i, cs := range consumers {
			cs := cs
			t.Run(fmt.Sprintf("read-%02d", i+1), func(t *testing.T) {
				t.Parallel()

				var gotMsgs []string
				for i := 0; i < N; i++ {
					select {
					case <-cs.Closed():
						t.Fatal("consumer was unexpectedly closed")

					case <-cs.ReachedEndOfTopic():
						t.Fatal("consumer unexpectedly reached end of topic")

					case msg := <-cs.Messages():
						if err := msg.Ack(); err != nil {
							t.Fatal(err)
						}
						gotMsgs = append(gotMsgs, string(msg.Payload))

					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}

				// ensure received messages match expected payloads
				if got, expected := len(gotMsgs), N; got != expected {
					t.Fatalf("received %d messages; expected %d", got, expected)
				}
			MATCH:
				for _, expected := range payloads {
					for _, got := range gotMsgs {
						if got == expected {
							continue MATCH
						}
					}
					t.Fatalf("msg %q expected, but not found in received messages", expected)
				}

				if err := cs.Close(ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

// TestClient_Int_ServerInitiatedTopicClose creates a producer and
// consumer. It then performs a topic "unload" request via the REST
// API, which will trigger a server-initiated close from Pulsar to
// the producer and consumer.
func TestClient_Int_ServerInitiatedTopicClose(t *testing.T) {
	addr := pulsarAddr(t)
	asyncErrs := make(chan error, 8)

	c, err := NewClient(ClientConfig{
		Addr: addr,
		Errs: asyncErrs,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for err := range asyncErrs {
			t.Log(err)
		}
	}()

	// context for remainder of test
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connected, err := c.Connect(ctx, "")
	if err != nil {
		t.Fatalf("client.Connect() err = %v", err)
	}
	t.Logf("CONNECTED: protocol=%d server=%q", connected.GetProtocolVersion(), connected.GetServerVersion())

	if err := c.Ping(ctx); err != nil {
		t.Fatalf("pinger.ping() err = %v", err)
	} else {
		t.Logf("PONG received")
	}

	topicName := fmt.Sprintf("test-%s", randString(32))
	topic := fmt.Sprintf("persistent://sample/standalone/ns1/%s", topicName)
	t.Logf("topic: %q", topic)

	topicResp, err := c.LookupTopic(ctx, topic, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, expected := topicResp.GetResponse(), api.CommandLookupTopicResponse_Connect; got != expected {
		t.Fatalf("topic lookup response = %q; expected %q", got, expected)
	}
	t.Log(topicResp.String())

	subscriptionName := randString(32)
	topicConsumer, err := c.NewExclusiveConsumer(ctx, topic, subscriptionName, 1)
	if err != nil {
		t.Fatal(err)
	}

	producerName := randString(32)
	topicProducer, err := c.NewProducer(ctx, topic, producerName)
	if err != nil {
		t.Fatal(err)
	}

	// unload performs an "unload" request to the
	// pulsar server via its HTTP REST interface
	unload := func() error {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return err
		}

		restAddr := net.JoinHostPort(host, "8080")
		url := fmt.Sprintf("http://%s/admin/namespaces/sample/standalone/ns1/unload", restAddr)
		t.Logf("Unload URL = %q", url)
		unloadReq, _ := http.NewRequest(http.MethodPut, url, nil)
		unloadReq.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(unloadReq)
		if err != nil {
			return err
		}
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("REST response: %q\n%s", resp.Status, body)
		}
		t.Logf("Unload response: %v\n%s", resp, body)

		return nil
	}

	unloadErr := make(chan error, 1)
	go func() {
		if err := unload(); err != nil {
			unloadErr <- err
		}
	}()

	select {
	case <-topicConsumer.Closed():
		t.Logf("consumer was successfully closed")
	case err := <-unloadErr:
		t.Fatalf("error sending unload REST API command: %+v", err)
	case <-ctx.Done():
		t.Fatal("consumer was not closed")
	}

	select {
	case <-topicProducer.Closed():
		t.Logf("producer was successfully closed")
	case err := <-unloadErr:
		t.Fatalf("error sending unload REST API command: %+v", err)
	case <-ctx.Done():
		t.Fatal("producer was not closed")
	}
}

// TestClient_Int_Unsubscribe creates a consumer and
// then unsubscribes it.
func TestClient_Int_Unsubscribe(t *testing.T) {
	asyncErrs := make(chan error, 8)
	go func() {
		for err := range asyncErrs {
			t.Log(err)
		}
	}()

	N := 4
	t.Run("group", func(t *testing.T) {
		for i := 0; i < N; i++ {
			t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
				t.Parallel()

				c, err := NewClient(ClientConfig{
					Addr: pulsarAddr(t),
					Errs: asyncErrs,
				})
				if err != nil {
					t.Fatal(err)
				}

				// context for remainder of test
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				connected, err := c.Connect(ctx, "")
				if err != nil {
					t.Fatalf("client.Connect() err = %v", err)
				}
				t.Logf("CONNECTED: protocol=%d server=%q", connected.GetProtocolVersion(), connected.GetServerVersion())

				if err := c.Ping(ctx); err != nil {
					t.Fatalf("pinger.ping() err = %v", err)
				} else {
					t.Logf("PONG received")
				}

				topic := fmt.Sprintf("persistent://sample/standalone/ns1/test-%s", randString(32))
				t.Logf("test topic: %q", topic)

				topicResp, err := c.LookupTopic(ctx, topic, false)
				if err != nil {
					t.Fatal(err)
				}
				// It's been observed that topic lookups will sometimes initially fail.
				// Here we check for those failures and retry with a small timeout to give
				// Pulsar a chance to catch-up.
				if got, expected := topicResp.GetResponse(), api.CommandLookupTopicResponse_Connect; got != expected {
					t.Logf("RETRYING topic %q", topic)
					time.Sleep(time.Millisecond * 250)

					topicResp, err = c.LookupTopic(ctx, topic, false)
					if err != nil {
						t.Fatal(err)
					}
					if got, expected := topicResp.GetResponse(), api.CommandLookupTopicResponse_Connect; got != expected {
						t.Fatalf("topic lookup response = %q; expected %q", got, expected)
					}
				}
				t.Log(topicResp.String())

				topicConsumer, err := c.NewExclusiveConsumer(ctx, topic, randString(32), 1)
				if err != nil {
					t.Fatal(err)
				}

				if err := topicConsumer.Unsubscribe(ctx); err != nil {
					t.Fatalf("Consumer.Unsubscribe() err = %+v; nil expected", err)
				}
			})
		}
	})
}

// TestClient_Int_RedeliverOverflow creates a producer and consumer with message
// buffer size of 1.
// Messages are created by the producer, and then it is asserted
// that discarded overflow messages are redelivered after calling
// RedeliverOverflow on the consumer.
func TestClient_Int_RedeliverOverflow(t *testing.T) {
	asyncErrs := make(chan error, 8)
	c, err := NewClient(ClientConfig{
		Addr: pulsarAddr(t),
		Errs: asyncErrs,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for err := range asyncErrs {
			t.Log(err)
		}
	}()

	// context for remainder of test
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	connected, err := c.Connect(ctx, "")
	if err != nil {
		t.Fatalf("client.Connect() err = %v", err)
	}
	t.Logf("CONNECTED: protocol=%d server=%q", connected.GetProtocolVersion(), connected.GetServerVersion())

	if err := c.Ping(ctx); err != nil {
		t.Fatalf("pinger.ping() err = %v", err)
	} else {
		t.Logf("PONG received")
	}

	topic := fmt.Sprintf("persistent://sample/standalone/ns1/test-%s", randString(32))
	t.Logf("test topic: %q", topic)

	topicResp, err := c.LookupTopic(ctx, topic, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, expected := topicResp.GetResponse(), api.CommandLookupTopicResponse_Connect; got != expected {
		t.Fatalf("topic lookup response = %q; expected %q", got, expected)
	}
	t.Log(topicResp.String())

	// number of messages to produce (and consume)
	N := 8
	payloads := make([]string, N)
	for i := 0; i < N; i++ {
		payloads[i] = fmt.Sprintf("%02d - Hola mundo, un mensaje asi", i)
	}

	// create single producer
	topicProducer, err := c.NewProducer(ctx, topic, "test")
	if err != nil {
		t.Fatal(err)
	}

	// create single consumer with buffer size 1
	cs, err := c.NewSharedConsumer(ctx, topic, randString(16), 1)
	if err != nil {
		t.Fatal(err)
	}

	// Produce messages

	for i := 0; i < N; i++ {
		if _, err := topicProducer.Send(ctx, []byte(payloads[i])); err != nil {
			t.Fatal(err)
		}
	}
	if err := topicProducer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Consume messages

	var gotMsgs []string
	for len(gotMsgs) < N {
		// Notify pulsar server that it can send us N messages
		if err := cs.Flow(uint32(N)); err != nil {
			t.Fatal(err)
		}

		select {
		case <-cs.Closed():
			t.Fatal("consumer was unexpectedly closed")

		case <-cs.ReachedEndOfTopic():
			t.Fatal("consumer unexpectedly reached end of topic")

		case msg := <-cs.Messages():
			if err := msg.Ack(); err != nil {
				t.Fatal(err)
			}
			gotMsgs = append(gotMsgs, string(msg.Payload[:]))
			t.Logf("got message: %q", string(msg.Payload))

			// allow messages to overflow
			time.Sleep(time.Millisecond * 250)

			if _, err := cs.RedeliverOverflow(ctx); err != nil {
				t.Fatal(err)
			}

		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	// ensure received messages match expected payloads
	if got, expected := len(gotMsgs), N; got != expected {
		t.Fatalf("received %d messages; expected %d", got, expected)
	}
MATCH:
	for _, expected := range payloads {
		for _, got := range gotMsgs {
			if got == expected {
				continue MATCH
			}
		}
		t.Fatalf("msg %q expected, but not found in received messages:\n%v", expected, gotMsgs)
	}

	if err := cs.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestClient_Int_RedeliverAll creates a producer and consumer with message
// buffer size of 1.
// Messages are created by the producer, and then it is asserted
// that discarded overflow messages are redelivered after calling
// RedeliverOverflow on the consumer.
func TestClient_Int_RedeliverAll(t *testing.T) {
	asyncErrs := make(chan error, 8)
	c, err := NewClient(ClientConfig{
		Addr: pulsarAddr(t),
		Errs: asyncErrs,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for err := range asyncErrs {
			t.Log(err)
		}
	}()

	// context for remainder of test
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	connected, err := c.Connect(ctx, "")
	if err != nil {
		t.Fatalf("client.Connect() err = %v", err)
	}
	t.Logf("CONNECTED: protocol=%d server=%q", connected.GetProtocolVersion(), connected.GetServerVersion())

	if err := c.Ping(ctx); err != nil {
		t.Fatalf("pinger.ping() err = %v", err)
	} else {
		t.Logf("PONG received")
	}

	topic := fmt.Sprintf("persistent://sample/standalone/ns1/test-%s", randString(32))
	t.Logf("test topic: %q", topic)

	topicResp, err := c.LookupTopic(ctx, topic, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, expected := topicResp.GetResponse(), api.CommandLookupTopicResponse_Connect; got != expected {
		t.Fatalf("topic lookup response = %q; expected %q", got, expected)
	}
	t.Log(topicResp.String())

	// number of messages to produce (and consume)
	N := 8
	payloads := make([]string, N)
	for i := 0; i < N; i++ {
		payloads[i] = fmt.Sprintf("%02d - Hola mundo, esto es un mensaje", i)
	}

	// create single producer
	topicProducer, err := c.NewProducer(ctx, topic, "test")
	if err != nil {
		t.Fatal(err)
	}

	// create single consumer with buffer size N
	cs, err := c.NewExclusiveConsumer(ctx, topic, randString(16), N)
	if err != nil {
		t.Fatal(err)
	}

	// Produce messages

	for i := 0; i < N; i++ {
		if _, err := topicProducer.Send(ctx, []byte(payloads[i])); err != nil {
			t.Fatal(err)
		}
	}
	if err := topicProducer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Consume messages

	// number of times to request unacknowledged messages
	M := 3

	var gotMsgs []string
	for i := 0; i < M; i++ {
		if err := cs.RedeliverUnacknowledged(ctx); err != nil {
			t.Fatalf("RedeliverUnacknowledged() err = %v; nil expected", err)
		}

		// Notify pulsar server that it can send us N messages
		if err := cs.Flow(uint32(N)); err != nil {
			t.Fatal(err)
		}

		for j := 0; j < N; j++ {
			select {
			case <-cs.Closed():
				t.Fatal("consumer was unexpectedly closed")

			case <-cs.ReachedEndOfTopic():
				t.Fatal("consumer unexpectedly reached end of topic")

			case msg := <-cs.Messages():
				// Message is _not_ acked
				gotMsgs = append(gotMsgs, string(msg.Payload[:]))
				t.Logf("got message: %q", string(msg.Payload))

			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}

	// Ensure received messages match expected payloads

	if got, expected := len(gotMsgs), N*M; got != expected {
		t.Fatalf("received %d messages; expected %d", got, expected)
	}
MATCH:
	for _, expected := range payloads {
		var found int
		for _, got := range gotMsgs {
			if got == expected {
				if found++; found == M {
					continue MATCH
				}
			}
		}
		t.Fatalf("msg %q expected, but not found in received messages:\n%v", expected, gotMsgs)
	}

	if err := cs.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
