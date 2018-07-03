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
	"testing"
	"time"
)

func TestManagedConsumer_Int_ReceiveAsync(t *testing.T) {
	// context for remainder of test
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	asyncErrs := make(chan error, 8)
	go func() {
		for err := range asyncErrs {
			t.Log(err)
		}
	}()

	topic := fmt.Sprintf("persistent://sample/standalone/ns1/test-%s", randString(32))
	cp := NewManagedClientPool()

	mcCfg := ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: pulsarAddr(t),
			Errs: asyncErrs,
		},
	}

	producerCfg := ManagedProducerConfig{
		ManagedClientConfig: mcCfg,
		Name:                randString(8),
		Topic:               topic,
	}
	mp := NewManagedProducer(cp, producerCfg)

	consumerCfg := ManagedConsumerConfig{
		ManagedClientConfig: mcCfg,
		Name:                randString(8),
		Topic:               topic,
		QueueSize:           128,
	}
	mc := NewManagedConsumer(cp, consumerCfg)
	errs := make(chan error, 1)
	received := make(chan Message, 16)
	go func() {
		errs <- mc.ReceiveAsync(ctx, received)
	}()

	expected := make([]string, 2048)
	for i := range expected {
		expected[i] = fmt.Sprintf("expected message #%03d", i)
	}

	// Allow consumer time to connect
	time.Sleep(time.Millisecond * 500)

	// Send a large number of messages
	go func() {
	MORE:
		for _, msg := range expected {
			for {
				if _, err := mp.Send(ctx, []byte(msg)); err != nil {
					continue
				}
				continue MORE
			}
		}
	}()

	// Pull received messages off channel and store
	// in slice
	var got []string
MSGS:
	for {
		select {
		case m := <-received:
			if err := mc.Ack(ctx, m); err != nil {
				t.Fatal(err)
			}

			got = append(got, string(m.Payload))
			if l := len(got); l == len(expected) {
				break MSGS
			}

		case err := <-errs:
			// either a consumer or producer error occurred
			t.Fatal(err)
		}
	}

	// Ensure all expected messages were received
EXPECTED:
	for _, expectedMsg := range expected {
		for _, gotMsg := range got {
			if gotMsg == expectedMsg {
				continue EXPECTED
			}
		}
		t.Fatalf("expected message %q not received", expectedMsg)
	}
}
