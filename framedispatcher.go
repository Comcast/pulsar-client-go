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
	"errors"
	"fmt"
	"sync"
)

// newFrameDispatcher returns an instantiated frameDispatcher.
func newFrameDispatcher() *frameDispatcher {
	return &frameDispatcher{
		prodSeqIDs: make(map[prodSeqKey]asyncResp),
		reqIDs:     make(map[uint64]asyncResp),
	}
}

// frameDispatcher is responsible for handling the request/response
// state of outstanding requests. It allows for users of this
// type to present a synchronous interface to an asynchronous
// process.
type frameDispatcher struct {
	// Connected and Pong responses have no requestID,
	// therefore a single channel is used as their
	// respective frameDispatcher. If the channel is
	// nil, there's no outstanding request.
	globalMu sync.Mutex // protects following
	global   *asyncResp

	// All responses that are correlated by their
	// requestID
	reqIDMu sync.Mutex // protects following
	reqIDs  map[uint64]asyncResp

	// All responses that are correlated by their
	// (producerID, sequenceID) tuple
	prodSeqIDsMu sync.Mutex // protects following
	prodSeqIDs   map[prodSeqKey]asyncResp
}

// asyncResp manages the state between a request
// and response. Requestors wait on the `resp` channel
// for the corresponding response frame to their request.
// If they are no longer interested in the response (timeout),
// then the `done` channel is closed, signaling to the response
// side that the response is not expected/needed.
type asyncResp struct {
	resp chan<- Frame
	done <-chan struct{}
}

// prodSeqKey is a composite lookup key for the dispatchers
// that use producerID and sequenceID to correlate responses,
// which are the SendReceipt and SendError responses.
type prodSeqKey struct {
	producerID uint64
	sequenceID uint64
}

// registerGlobal is used to wait for responses that have no identifying
// id (Pong, Connected responses). Only one outstanding global request
// is allowed at a time. Callers should always call cancel, specifically
// when they're not interested in the response.
func (f *frameDispatcher) registerGlobal() (response <-chan Frame, cancel func(), err error) {
	var mu sync.Mutex
	done := make(chan struct{})
	cancel = func() {
		mu.Lock()
		defer mu.Unlock()
		if done == nil {
			return
		}

		f.globalMu.Lock()
		f.global = nil
		f.globalMu.Unlock()

		close(done)
		done = nil
	}

	resp := make(chan Frame)

	f.globalMu.Lock()
	if f.global != nil {
		f.globalMu.Unlock()
		return nil, nil, errors.New("outstanding global request already in progress")
	}
	f.global = &asyncResp{
		resp: resp,
		done: done,
	}
	f.globalMu.Unlock()

	return resp, cancel, nil
}

// notifyGlobal should be called with response frames that have
// no identifying id (Pong, Connected).
func (f *frameDispatcher) notifyGlobal(frame Frame) error {
	f.globalMu.Lock()
	a := f.global
	// ensure additional calls to notify
	// fail with UnexpectedMsg (unless register is called again)
	f.global = nil
	f.globalMu.Unlock()

	if a == nil {
		return newErrUnexpectedMsg(frame.BaseCmd.GetType())
	}

	select {
	case a.resp <- frame:
		// sent response back to sender
		return nil
	case <-a.done:
		return newErrUnexpectedMsg(frame.BaseCmd.GetType())
	}
}

// registerProdSeqID is used to wait for responses that have (producerID, sequenceID)
// id tuples to correlate them to their request. Callers should always call cancel,
// specifically when they're not interested in the response. It is an error
// to have multiple outstanding requests with the same id tuple.
func (f *frameDispatcher) registerProdSeqIDs(producerID, sequenceID uint64) (response <-chan Frame, cancel func(), err error) {
	key := prodSeqKey{producerID, sequenceID}

	var mu sync.Mutex
	done := make(chan struct{})
	cancel = func() {
		mu.Lock()
		defer mu.Unlock()
		if done == nil {
			return
		}

		f.prodSeqIDsMu.Lock()
		delete(f.prodSeqIDs, key)
		f.prodSeqIDsMu.Unlock()

		close(done)
		done = nil
	}

	resp := make(chan Frame)

	f.prodSeqIDsMu.Lock()
	if _, ok := f.prodSeqIDs[key]; ok {
		f.prodSeqIDsMu.Unlock()
		return nil, nil, fmt.Errorf("already exists an outstanding response for producerID %d, sequenceID %d", producerID, sequenceID)
	}
	f.prodSeqIDs[key] = asyncResp{
		resp: resp,
		done: done,
	}
	f.prodSeqIDsMu.Unlock()

	return resp, cancel, nil
}

// notifyProdSeqIDs should be called with response frames that have
// (producerID, sequenceID) id tuples to correlate them to their requests.
func (f *frameDispatcher) notifyProdSeqIDs(producerID, sequenceID uint64, frame Frame) error {
	key := prodSeqKey{producerID, sequenceID}

	f.prodSeqIDsMu.Lock()
	// fetch response channel from cubbyhole
	a, ok := f.prodSeqIDs[key]
	// ensure additional calls to notify with same key will
	// fail with UnexpectedMsg (unless registerProdSeqIDs with same key is called)
	delete(f.prodSeqIDs, key)
	f.prodSeqIDsMu.Unlock()

	if !ok {
		return newErrUnexpectedMsg(frame.BaseCmd.GetType(), producerID, sequenceID)
	}

	select {
	case a.resp <- frame:
		// response was correctly pushed into channel
		return nil
	case <-a.done:
		return newErrUnexpectedMsg(frame.BaseCmd.GetType(), producerID, sequenceID)
	}
}

// registerReqID is used to wait for responses that have a requestID
// id to correlate them to their request. Callers should always call cancel,
// specifically when they're not interested in the response. It is an error
// to have multiple outstanding requests with the id.
func (f *frameDispatcher) registerReqID(requestID uint64) (response <-chan Frame, cancel func(), err error) {
	var mu sync.Mutex
	done := make(chan struct{})
	cancel = func() {
		mu.Lock()
		defer mu.Unlock()
		if done == nil {
			return
		}

		f.reqIDMu.Lock()
		delete(f.reqIDs, requestID)
		f.reqIDMu.Unlock()

		close(done)
		done = nil
	}

	resp := make(chan Frame)

	f.reqIDMu.Lock()
	if _, ok := f.reqIDs[requestID]; ok {
		f.reqIDMu.Unlock()
		return nil, nil, fmt.Errorf("already exists an outstanding response for requestID %d", requestID)
	}
	f.reqIDs[requestID] = asyncResp{
		resp: resp,
		done: done,
	}
	f.reqIDMu.Unlock()

	return resp, cancel, nil
}

// notifyReqID should be called with response frames that have
// a requestID to correlate them to their requests.
func (f *frameDispatcher) notifyReqID(requestID uint64, frame Frame) error {
	f.reqIDMu.Lock()
	// fetch response channel from cubbyhole
	a, ok := f.reqIDs[requestID]
	// ensure additional calls to notifyReqID with same key will
	// fail with UnexpectedMsg (unless addReqID with same key is called)
	delete(f.reqIDs, requestID)
	f.reqIDMu.Unlock()

	if !ok {
		return newErrUnexpectedMsg(frame.BaseCmd.GetType(), requestID)
	}

	// send received message to response channel
	select {
	case a.resp <- frame:
		// response was correctly pushed into channel
		return nil
	case <-a.done:
		return newErrUnexpectedMsg(frame.BaseCmd.GetType(), requestID)
	}
}
