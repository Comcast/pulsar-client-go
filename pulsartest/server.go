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

package pulsartest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/Comcast/pulsar-client-go/api"
)

// NewServer returns a ready-to-use Pulsar test server.
// The server will be closed when the context is canceled.
func NewServer(ctx context.Context) (*Server, error) {
	l, err := net.ListenTCP("tcp4", &net.TCPAddr{
		IP:   net.IPv4zero,
		Port: 0, // Let the OS pick a random free port
	})
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	// buffer a reasonable (and arbitrary) amount
	// so that all frames received during a single test
	// will be buffered.
	received := make(chan Frame, 128)

	srv := Server{
		Addr:             fmt.Sprintf("pulsar://%s", l.Addr().String()),
		Received:         received,
		topicLookupResps: make(map[string]topicLookupResp),
		conns:            make(map[string]net.Conn),
	}

	// accept new connections in new goroutine
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}

			remoteAddr := c.RemoteAddr().String()

			srv.mu.Lock()
			srv.totalConns++
			srv.conns[remoteAddr] = c
			srv.mu.Unlock()

			// close all connections when
			// context is canceled
			go func() {
				<-ctx.Done()
				_ = c.Close()
			}()

			// handle individual connection
			go func(c net.Conn, remoteAddr string) {
				defer func() {
					// cleanup connection
					_ = c.Close()
					srv.mu.Lock()
					delete(srv.conns, remoteAddr)
					srv.mu.Unlock()
				}()

				for {
					var f Frame
					if err := f.Decode(c); err != nil {
						return
					}

					if resp := srv.handleFrame(f, remoteAddr); resp != nil {
						if err := resp.Encode(c); err != nil {
							fmt.Fprintln(os.Stderr, err)
							return
						}
					}

					select {
					case received <- f: // received is buffered
					default:
						panic("unable to send frame to received chan")
					}
				}
			}(c, remoteAddr)
		}
	}()

	return &srv, nil
}

// Server emulates a Pulsar server
type Server struct {
	Addr     string
	Received <-chan Frame

	trmu             sync.Mutex
	topicLookupResps map[string]topicLookupResp // map of topic -> topicLookupResp

	imu            sync.Mutex // protects following
	ignoreConnects bool
	ignorePings    bool

	mu         sync.Mutex // protects following
	totalConns int
	conns      map[string]net.Conn
}

type topicLookupResp struct {
	respType               api.CommandLookupTopicResponse_LookupType
	brokerServiceURL       string
	proxyThroughServiceURL bool
}

// AssertReceived accepts a list of message types. It then compares them to the frames
// it has received, and returns an error if they don't match or if the context times
// out. Note: The m.Received channel is buffered, so AssertReceived can be called from the
// main goroutine.
func (m *Server) AssertReceived(ctx context.Context, frameTypes ...api.BaseCommand_Type) error {
	for _, expected := range frameTypes {
		select {
		case f := <-m.Received:
			if got := f.BaseCmd.GetType(); got != expected {
				return fmt.Errorf("got frame of type %q; expected type %q", got, expected)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// SetIgnorePings instructs the server to NOT respond
// to PING requests if true.
func (m *Server) SetIgnorePings(ignore bool) {
	m.imu.Lock()
	defer m.imu.Unlock()
	m.ignorePings = ignore
}

// SetIgnoreConnects instructs the server to NOT respond
// to CONNECT requests if true.
func (m *Server) SetIgnoreConnects(ignore bool) {
	m.imu.Lock()
	defer m.imu.Unlock()
	m.ignoreConnects = ignore
}

// SetTopicLookupResp updates the BrokerServiceURL returned for
// the given topic from LOOKUP requests. If not set, the server's
// Addr is used by default. If connect if false, the response type
// is REDIRECT.
func (m *Server) SetTopicLookupResp(topic, serviceURL string, respType api.CommandLookupTopicResponse_LookupType, proxyThroughServiceURL bool) {
	m.trmu.Lock()
	m.topicLookupResps[topic] = topicLookupResp{
		respType:               respType,
		brokerServiceURL:       serviceURL,
		proxyThroughServiceURL: proxyThroughServiceURL,
	}
	m.trmu.Unlock()
}

// TotalNumConns returns the total number of connections
// (active or inactive) received by the Server.
func (m *Server) TotalNumConns() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalConns
}

// Broadcast sends the given frame to all connected clients.
func (m *Server) Broadcast(f Frame) error {
	var b bytes.Buffer
	if err := f.Encode(&b); err != nil {
		return err
	}
	data := b.Bytes()

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, c := range m.conns {
		if _, err := io.Copy(c, bytes.NewReader(data)); err != nil {
			return err
		}
	}

	return nil
}

// CloseAll closes all connected client connections.
func (m *Server) CloseAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, c := range m.conns {
		if err := c.Close(); err != nil {
			return err
		}
		delete(m.conns, c.RemoteAddr().String())
	}

	return nil
}

func (m *Server) handleFrame(f Frame, remoteAddr string) *Frame {
	msgType := f.BaseCmd.GetType()

	switch msgType {

	case api.BaseCommand_CONNECT:
		m.imu.Lock()
		ignore := m.ignoreConnects
		m.imu.Unlock()

		if ignore {
			return nil
		}

		return &Frame{
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_CONNECTED.Enum(),
				Connected: &api.CommandConnected{
					ProtocolVersion: proto.Int32(10),
					ServerVersion:   proto.String("mock"),
				},
			},
		}

	case api.BaseCommand_PING:
		m.imu.Lock()
		ignore := m.ignorePings
		m.imu.Unlock()

		if ignore {
			return nil
		}

		return &Frame{
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_PONG.Enum(),
				Pong: &api.CommandPong{},
			},
		}

	case api.BaseCommand_LOOKUP:
		lookup := f.BaseCmd.GetLookupTopic()

		// default response
		resp := topicLookupResp{
			respType:               api.CommandLookupTopicResponse_Connect,
			brokerServiceURL:       m.Addr,
			proxyThroughServiceURL: false,
		}

		// check if override response is set for topic
		m.trmu.Lock()
		if topicResp, ok := m.topicLookupResps[lookup.GetTopic()]; ok {
			resp = topicResp
		}
		m.trmu.Unlock()

		return &Frame{
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_LOOKUP_RESPONSE.Enum(),
				LookupTopicResponse: &api.CommandLookupTopicResponse{
					Response:               resp.respType.Enum(),
					RequestId:              lookup.RequestId,
					Authoritative:          proto.Bool(true),
					BrokerServiceUrl:       proto.String(resp.brokerServiceURL),
					ProxyThroughServiceUrl: proto.Bool(resp.proxyThroughServiceURL),
					Error: func() *api.ServerError {
						if resp.respType != api.CommandLookupTopicResponse_Failed {
							return nil
						}
						return api.ServerError_TopicNotFound.Enum()
					}(),
				},
			},
		}

	// allow Producers to be created
	case api.BaseCommand_PRODUCER:
		return &Frame{
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_PRODUCER_SUCCESS.Enum(),
				ProducerSuccess: &api.CommandProducerSuccess{
					RequestId:    f.BaseCmd.GetProducer().RequestId,
					ProducerName: proto.String("test"),
				},
			},
		}

	// allow Consumers to be created
	case api.BaseCommand_SUBSCRIBE:
		return &Frame{
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_SUCCESS.Enum(),
				Success: &api.CommandSuccess{
					RequestId: f.BaseCmd.GetSubscribe().RequestId,
				},
			},
		}

	case api.BaseCommand_SEND:
		return &Frame{
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_SEND_RECEIPT.Enum(),
				SendReceipt: &api.CommandSendReceipt{
					ProducerId: f.BaseCmd.GetSend().ProducerId,
					SequenceId: f.BaseCmd.GetSend().SequenceId,
				},
			},
		}

	default:
		return nil
	}
}
