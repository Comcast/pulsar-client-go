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
	"errors"
	"sync"
	"time"
)

// ManagedClientConfig is used to configure a ManagedClient.
type ManagedClientConfig struct {
	ClientConfig

	PingFrequency         time.Duration // how often to PING server
	PingTimeout           time.Duration // how long to wait for PONG response
	ConnectTimeout        time.Duration // how long to wait for CONNECTED response
	InitialReconnectDelay time.Duration // how long to initially wait to reconnect Client
	MaxReconnectDelay     time.Duration // maximum time to wait to attempt to reconnect Client
}

// setDefaults returns a modified config with appropriate zero values set to defaults.
func (m ManagedClientConfig) setDefaults() ManagedClientConfig {
	if m.PingFrequency <= 0 {
		m.PingFrequency = 30 * time.Second // default used by Java client
	}
	if m.PingTimeout <= 0 {
		m.PingTimeout = m.PingFrequency / 2
	}
	if m.ConnectTimeout <= 0 {
		m.ConnectTimeout = 5 * time.Second
	}
	if m.InitialReconnectDelay <= 0 {
		m.InitialReconnectDelay = 1 * time.Second
	}
	if m.MaxReconnectDelay <= 0 {
		m.MaxReconnectDelay = 2 * time.Minute
	}
	return m
}

// NewManagedClient returns a ManagedClient for the given address. The
// Client will be created and monitored in the background.
func NewManagedClient(cfg ManagedClientConfig) *ManagedClient {
	cfg = cfg.setDefaults()

	m := ManagedClient{
		cfg:       cfg,
		asyncErrs: asyncErrors(cfg.Errs),
		donec:     make(chan struct{}),
		waitc:     make(chan struct{}),
	}

	// continuously create and re-create
	// the client when necessary until Stop
	// is called
	go m.manage()

	return &m
}

// ManagedClient wraps a Client with re-connect and
// connection management logic.
type ManagedClient struct {
	cfg ManagedClientConfig

	asyncErrs asyncErrors

	mu     sync.RWMutex // protects following
	isDone bool
	donec  chan struct{}
	client *Client       // either client is nil and wait isn't or vice versa
	waitc  chan struct{} // if client is nil, this will unblock when it's been re-set
}

// Stop closes the Client if possible, and/or stops
// it from re-connecting. The ManagedClient shouldn't be used
// after calling Stop.
func (m *ManagedClient) Stop() error {
	m.mu.RLock()

	if !m.isDone {
		m.isDone = true
		close(m.donec)
	}

	m.mu.RUnlock()
	return nil
}

// Done returns a channel that unblocks when the ManagedClient
// has been closed.
func (m *ManagedClient) Done() <-chan struct{} {
	return m.donec
}

// Get returns the managed Client in a thread-safe way. If the client
// is temporarily unavailable, Get will block until either it becomes
// available or the context expires.
//
// There is no guarantee that the returned Client will
// be connected or stay connected.
func (m *ManagedClient) Get(ctx context.Context) (*Client, error) {
	for {
		m.mu.RLock()
		client := m.client
		wait := m.waitc
		done := m.donec
		m.mu.RUnlock()

		if client != nil {
			return client, nil
		}

		select {
		case <-wait:
			// A new client was created.
			// Re-enter read-lock to obtain it.
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-done:
			return nil, errors.New("managed client is stopped")
		}
	}
}

// set unblocks the "wait" channel (if not nil),
// and sets the client under lock.
func (m *ManagedClient) set(c *Client) {
	m.mu.Lock()

	m.client = c

	if m.waitc != nil {
		close(m.waitc)
		m.waitc = nil
	}

	m.mu.Unlock()
}

// unset creates the "wait" channel (if nil),
// and sets the client to nil under lock.
func (m *ManagedClient) unset() {
	m.mu.Lock()

	if m.waitc == nil {
		// allow unset() to be called
		// multiple times by only creating
		// wait chan if its nil
		m.waitc = make(chan struct{})
	}
	m.client = nil

	m.mu.Unlock()
}

// newClient attempts to create a Client and perform a Connect request.
func (m *ManagedClient) newClient(ctx context.Context) (*Client, error) {
	client, err := NewClient(m.cfg.ClientConfig)
	if err != nil {
		return nil, err
	}

	// Connect is required before sending any other type of message.

	// If the physical address is not the same as the service address,
	// then we are connecting through a proxy and must specify the target
	// broker in the connect message.
	var proxyBrokerURL string
	if m.cfg.phyAddr != m.cfg.Addr {
		proxyBrokerURL = m.cfg.Addr
	}
	if m.cfg.tls() {
		_, err = client.ConnectTLS(ctx, proxyBrokerURL)
	} else {
		_, err = client.Connect(ctx, proxyBrokerURL)
	}

	if err != nil {
		_ = client.Close()
		return nil, err
	}

	return client, nil
}

// reconnect blocks while a new client is created.
// Nil will be returned if and only if Stop() was
// called.
func (m *ManagedClient) reconnect(initial bool) *Client {
	retryDelay := m.cfg.InitialReconnectDelay

	for attempt := 1; ; attempt++ {
		// Don't delay if this is the initial
		// connect attempt
		if initial {
			initial = false
			select {
			case <-m.donec:
				return nil
			default:
			}
		} else {
			select {
			case <-time.After(retryDelay):
			case <-m.donec:
				return nil
			}
		}

		if retryDelay < m.cfg.MaxReconnectDelay {
			// double retry delay until we reach the max
			if retryDelay *= 2; retryDelay > m.cfg.MaxReconnectDelay {
				retryDelay = m.cfg.MaxReconnectDelay
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), m.cfg.ConnectTimeout)
		newClient, err := m.newClient(ctx)
		cancel()
		if err != nil {
			m.asyncErrs.send(err)
			continue
		}

		return newClient
	}
}

// managed monitors the Client for conditions that require it to
// be re-created.
func (m *ManagedClient) manage() {
	defer m.unset()

	client := m.reconnect(true)
	if client == nil {
		// client == nil only if the ManagedClient
		// was stopped
		return
	}
	m.set(client)

	pingTick := time.NewTicker(m.cfg.PingFrequency)
	defer pingTick.Stop()

	// Enter a loop to watch the client for any
	// conditions that require it to be re-created.
	for {
		select {
		// managed client was stopped.
		// exit
		case <-m.donec:
			if err := client.Close(); err != nil {
				m.asyncErrs.send(err)
			}
			return

		// client was closed
		case <-client.Closed():
			// reconnect

		// try to ping server
		// if failure, reconnect
		case <-pingTick.C:
			ctx, cancel := context.WithTimeout(context.Background(), m.cfg.PingTimeout)
			err := client.Ping(ctx)
			cancel()
			if err == nil {
				// ping success, no reconnect
				continue
			}
			m.asyncErrs.send(err)

			if err = client.Close(); err != nil {
				m.asyncErrs.send(err)
			}

		}

		// If we've made it here, the client needs to
		// be re-created

		m.unset()
		if client = m.reconnect(false); client == nil {
			// client == nil only if the ManagedClient
			// was stopped
			return
		}
		m.set(client)
	}
}
