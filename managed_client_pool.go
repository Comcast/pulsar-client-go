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
	"strings"
	"sync"
	"time"

	"github.com/Comcast/pulsar-client-go/api"
)

// NewManagedClientPool initializes a ManagedClientPool.
func NewManagedClientPool() *ManagedClientPool {
	return &ManagedClientPool{
		pool: make(map[clientPoolKey]*ManagedClient),
	}
}

// ManagedClientPool provides a thread-safe store for ManagedClients,
// based on their address.
type ManagedClientPool struct {
	mu   sync.RWMutex                     // protects following
	pool map[clientPoolKey]*ManagedClient // key -> managedClient
}

// clientPoolKey defines the unique attributes of a client
type clientPoolKey struct {
	logicalAddr string
	phyAddr     string
	dialTimeout time.Duration
	tlsCertFile string
	tlsKeyFile  string

	pingFrequency         time.Duration
	pingTimeout           time.Duration
	connectTimeout        time.Duration
	initialReconnectDelay time.Duration
	maxReconnectDelay     time.Duration
}

// Get returns the ManagedClient for the given client configuration.
// First the cache is checked for an existing client. If one doesn't exist,
// a new one is created and cached, then returned.
func (m *ManagedClientPool) Get(cfg ManagedClientConfig) *ManagedClient {
	key := clientPoolKey{
		logicalAddr:           strings.TrimPrefix(cfg.Addr, "pulsar://"),
		dialTimeout:           cfg.DialTimeout,
		tlsCertFile:           cfg.TLSCertFile,
		tlsKeyFile:            cfg.TLSKeyFile,
		pingFrequency:         cfg.PingFrequency,
		pingTimeout:           cfg.PingTimeout,
		connectTimeout:        cfg.ConnectTimeout,
		initialReconnectDelay: cfg.InitialReconnectDelay,
		maxReconnectDelay:     cfg.MaxReconnectDelay,
	}

	m.mu.RLock()
	mc, ok := m.pool[key]
	m.mu.RUnlock()

	if ok {
		return mc
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check locking
	if mc, ok = m.pool[key]; ok {
		return mc
	}

	mc = NewManagedClient(cfg)
	m.pool[key] = mc

	go func() {
		// Remove the ManagedClient from the
		// pool if/when it is stopped.
		<-mc.Done()

		m.mu.Lock()
		delete(m.pool, key)
		m.mu.Unlock()
	}()

	return mc
}

// maxTopicLookupRedirects defines an arbitrary maximum
// number of topic redirects to follow before erring, to
// prevent infinite redirect loops.
const maxTopicLookupRedirects = 8

// ForTopic performs topic lookup for the given topic and returns
// the ManagedClient for the discovered topic information.
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Topiclookup-6g0lo
// incubator-pulsar/pulsar-client/src/main/java/org/apache/pulsar/client/impl/BinaryProtoLookupService.java
func (m *ManagedClientPool) ForTopic(ctx context.Context, cfg ManagedClientConfig, topic string) (*ManagedClient, error) {
	// For initial lookup request, authoritative should == false
	var authoritative bool
	serviceAddr := cfg.Addr

	for redirects := 0; redirects < maxTopicLookupRedirects; redirects++ {
		mc := m.Get(cfg)
		client, err := mc.Get(ctx)
		if err != nil {
			return nil, err
		}

		// Topic lookup is required before producing and/or consuming.
		lookupResp, err := client.LookupTopic(ctx, topic, authoritative)
		if err != nil {
			return nil, err
		}

		// Response type can be:
		// - Connect
		// - Redirect
		// - Failed
		lookupType := lookupResp.GetResponse()
		authoritative = lookupResp.GetAuthoritative()

		if lookupType == api.CommandLookupTopicResponse_Failed {
			lookupErr := lookupResp.GetError()
			return nil, fmt.Errorf("(%s) %s", lookupErr.String(), lookupResp.GetMessage())
		}

		// Update configured address with address
		// provided in response
		if cfg.tls() {
			cfg.Addr = lookupResp.GetBrokerServiceUrlTls()
		} else {
			cfg.Addr = lookupResp.GetBrokerServiceUrl()
		}

		switch lookupType {
		case api.CommandLookupTopicResponse_Redirect:
			// Repeat process, but with new broker address

		case api.CommandLookupTopicResponse_Connect:
			// If ProxyThroughServiceUrl is true, then
			// the original address must be used for the physical
			// TCP connection. The lookup response address must then
			// be provided in the Connect command. But the broker service
			// address should be used by the pool as part of the lookup key.
			if lookupResp.GetProxyThroughServiceUrl() {
				cfg.phyAddr = serviceAddr
			}
			return m.Get(cfg), nil
		}
	}

	return nil, fmt.Errorf("max topic lookup redirects (%d) for topic %q", maxTopicLookupRedirects, topic)
}
