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

// This program offers a simple CLI utility for interacting
// with a Pulsar server using the `pulsar` package.
//
// It's main goal is to aid in testing and debugging of the `pulsar`
// package.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Comcast/pulsar-client-go"
)

var args = struct {
	pulsar    string
	tlsCert   string
	tlsKey    string
	name      string
	topic     string
	producer  bool
	message   string
	shared    bool
	autoACK   bool
	decodeJob bool
}{
	pulsar:    "localhost:6650",
	tlsCert:   "",
	tlsKey:    "",
	name:      "demo",
	topic:     "persistent://sample/standalone/ns1/demo",
	producer:  false,
	message:   "hola mundo",
	shared:    false,
	autoACK:   true,
	decodeJob: false,
}

func main() {
	flag.StringVar(&args.pulsar, "pulsar", args.pulsar, "pulsar address")
	flag.StringVar(&args.tlsCert, "tls-cert", args.tlsCert, "(optional) path to TLS certificate")
	flag.StringVar(&args.tlsKey, "tls-key", args.tlsKey, "(optional) path to TLS key")
	flag.StringVar(&args.name, "name", args.name, "producer/consumer name")
	flag.StringVar(&args.topic, "topic", args.topic, "producer/consumer topic")
	flag.BoolVar(&args.producer, "producer", args.producer, "if true, produce messages, otherwise consume")
	flag.StringVar(&args.message, "message", args.message, "message to send when producing (with %03d $messageNumber tacked on the front)")
	flag.BoolVar(&args.shared, "shared", args.shared, "if true, consumer is shared, otherwise exclusive")
	flag.BoolVar(&args.autoACK, "auto-ack", args.autoACK, "if true, consumed messages are automatically ACK'd. Otherwise, they're ACK'd after hitting ENTER")
	flag.BoolVar(&args.decodeJob, "job", args.decodeJob, "if true, consumed messages are decoded from Job protobuf format. Otherwise, they're printed as-is to STDOUT")
	flag.Parse()

	asyncErrs := make(chan error, 8)
	go func() {
		for err := range asyncErrs {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	mcp := pulsar.NewManagedClientPool()

	switch args.producer {
	case true:
		// Create the managed producer
		mpCfg := pulsar.ManagedProducerConfig{
			Name:                  args.name,
			Topic:                 args.topic,
			NewProducerTimeout:    time.Second,
			InitialReconnectDelay: time.Second,
			MaxReconnectDelay:     time.Minute,
			ManagedClientConfig: pulsar.ManagedClientConfig{
				ClientConfig: pulsar.ClientConfig{
					Addr:        args.pulsar,
					TLSCertFile: args.tlsCert,
					TLSKeyFile:  args.tlsKey,
					Errs:        asyncErrs,
				},
			},
		}
		mp := pulsar.NewManagedProducer(mcp, mpCfg)
		fmt.Printf("Created producer on topic %q...\n", args.topic)

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		var i int
		for {
			select {
			case <-ticker.C:
				sctx, cancel := context.WithTimeout(ctx, time.Second)
				_, err := mp.Send(sctx, []byte(fmt.Sprintf("%03d %s", i, args.message)))
				cancel()
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				i++

			case <-ctx.Done():
				return
			}
		}

	case false:
		// Create managed consumer
		mcCfg := pulsar.ManagedConsumerConfig{
			Name:                  args.name,
			Topic:                 args.topic,
			Exclusive:             !args.shared,
			NewConsumerTimeout:    time.Second,
			InitialReconnectDelay: time.Second,
			MaxReconnectDelay:     time.Minute,
			ManagedClientConfig: pulsar.ManagedClientConfig{
				ClientConfig: pulsar.ClientConfig{
					Addr:        args.pulsar,
					TLSCertFile: args.tlsCert,
					TLSKeyFile:  args.tlsKey,
					Errs:        asyncErrs,
				},
			},
		}
		mc := pulsar.NewManagedConsumer(mcp, mcCfg)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msg, err := mc.Receive(ctx)

			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			fmt.Println(string(msg.Payload))
			if err := msg.Ack(); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}
	}
}
