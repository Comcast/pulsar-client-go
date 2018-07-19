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
	"testing"
	"time"

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/Comcast/pulsar-client-go/pulsartest"
)

func TestManagedClientPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mcp := NewManagedClientPool()
	mc := mcp.Get(ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: srv.Addr,
		},
	})

	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
	}
	if err = srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}

	_, err = mc.Get(ctx)
	if err != nil {
		t.Fatalf("ManagedClient.Get() err = %v; expected nil", err)
	}

	// Assert additional call to Get returns same object
	mc2 := mcp.Get(ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: srv.Addr,
		},
	})
	if mc != mc2 {
		t.Fatalf("Get() returned %v; expected identical result from first call to Get() %v", mc2, mc)
	}
}

func TestManagedClientPool_Stop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mcp := NewManagedClientPool()
	mc := mcp.Get(ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: srv.Addr,
		},
	})

	expectedFrames := []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
	}
	if err := srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}

	mc.Stop()
	// Allow time for the ManagedClient to be removed
	// from the pool
	time.Sleep(200 * time.Millisecond)

	mc2 := mcp.Get(ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: srv.Addr,
		},
	})
	if mc == mc2 {
		t.Fatal("Get() returned same ManagedClient as previous; expected different object after calling ManagedClient.Stop()")
	}

	// Assert new ManagedClient connects to server
	expectedFrames = []api.BaseCommand_Type{
		api.BaseCommand_CONNECT,
	}
	if err := srv.AssertReceived(ctx, expectedFrames...); err != nil {
		t.Fatal(err)
	}
}

func TestManagedClientPool_ForTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	primarySrv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	topic := "test"

	cp := NewManagedClientPool()
	mc, err := cp.ForTopic(ctx, ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: primarySrv.Addr,
		},
	}, topic)
	if err != nil {
		t.Fatalf("ForTopic() err = %v; expected nil", err)
	}

	if got, expected := mc.cfg.connAddr(), primarySrv.Addr; got != expected {
		t.Fatalf("ManagedClient address = %q; expected %q", got, expected)
	} else {
		t.Logf("ManagedClient address = %q", got)
	}
}

func TestManagedClientPool_ForTopic_Failed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	primarySrv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	topic := "test"
	// set topic look response type to FAILED
	primarySrv.SetTopicLookupResp(topic, primarySrv.Addr, api.CommandLookupTopicResponse_Failed, false)

	cp := NewManagedClientPool()
	_, err = cp.ForTopic(ctx, ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: primarySrv.Addr,
		},
	}, topic)

	if err == nil {
		t.Fatalf("ForTopic() err = %v; expected non-nil", err)
	}
	t.Logf("ForTopic() err (expected) = %v", err)
}

func TestManagedClientPool_ForTopic_Proxy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	primarySrv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	topic := "test"
	brokerURL := "pulsar://broker-url"
	primarySrv.SetTopicLookupResp(topic, brokerURL, api.CommandLookupTopicResponse_Connect, true)

	cp := NewManagedClientPool()
	mc, err := cp.ForTopic(ctx, ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: primarySrv.Addr,
		},
	}, topic)
	if err != nil {
		t.Fatalf("ForTopic() err = %v; expected nil", err)
	}

	// original broker addr should be used as physical address
	if got, expected := mc.cfg.connAddr(), primarySrv.Addr; got != expected {
		t.Fatalf("ManagedClient address = %q; expected %q", got, expected)
	} else {
		t.Logf("ManagedClient address = %q", got)
	}

	// the response broker address should be used as lookup/logical address
	if got, expected := mc.cfg.Addr, brokerURL; got != expected {
		t.Fatalf("ManagedClient Addr = %q; expected %q", got, expected)
	} else {
		t.Logf("ManagedClient Addr = %q", got)
	}
}

func TestManagedClientPool_ForTopic_Connect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// create 2 servers
	// - primarySrv which returns CONNECT to
	// - topicSrv

	primarySrv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	topicSrv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	topic := "test"
	// Set primary server to redirect to topicSrv
	// for "test" topic
	primarySrv.SetTopicLookupResp(topic, topicSrv.Addr, api.CommandLookupTopicResponse_Connect, false)

	cp := NewManagedClientPool()

	mc, err := cp.ForTopic(ctx, ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: primarySrv.Addr,
		},
	}, topic)
	if err != nil {
		t.Fatalf("ForTopic() err = %v; expected nil", err)
	}

	if got, expected := mc.cfg.connAddr(), topicSrv.Addr; got != expected {
		t.Fatalf("ManagedClient address = %q; expected %q", got, expected)
	} else {
		t.Logf("redirected ManagedClient address = %q", got)
	}
}

func TestManagedClientPool_ForTopic_Redirect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// create 2 servers
	// - primarySrv which returns REDIRECT to
	// - topicSrv

	primarySrv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	topicSrv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	topic := "test"
	// Set primary server to redirect to topicSrv
	// for "test" topic
	primarySrv.SetTopicLookupResp(topic, topicSrv.Addr, api.CommandLookupTopicResponse_Redirect, false)

	cp := NewManagedClientPool()

	mc, err := cp.ForTopic(ctx, ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: primarySrv.Addr,
		},
	}, topic)
	if err != nil {
		t.Fatalf("ForTopic() err = %v; expected nil", err)
	}

	if got, expected := mc.cfg.connAddr(), topicSrv.Addr; got != expected {
		t.Fatalf("ManagedClient address = %q; expected %q", got, expected)
	} else {
		t.Logf("redirected ManagedClient address = %q", got)
	}
}

func TestManagedClientPool_ForTopic_RedirectLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// create 2 servers
	// - primarySrv which returns REDIRECT to
	// - topicSrv which redirects back to primarySrv (loop)

	primarySrv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	topicSrv, err := pulsartest.NewServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	topic := "test"
	// Set primary server to redirect to topicSrv
	primarySrv.SetTopicLookupResp(topic, topicSrv.Addr, api.CommandLookupTopicResponse_Redirect, false)
	// Set topicSrv to redirect back to primary, creating loop
	topicSrv.SetTopicLookupResp(topic, primarySrv.Addr, api.CommandLookupTopicResponse_Redirect, false)

	cp := NewManagedClientPool()

	_, err = cp.ForTopic(ctx, ManagedClientConfig{
		ClientConfig: ClientConfig{
			Addr: primarySrv.Addr,
		},
	}, topic)

	if err == nil {
		t.Fatalf("ForTopic() err = %v; expected non-nil", err)
	}
	t.Logf("ForTopic() err (expected) = %v", err)
}
