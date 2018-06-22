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
	"sync"
)

// newSubscriptions returns a ready-to-use subscriptions.
func newSubscriptions() *subscriptions {
	return &subscriptions{
		consumers: make(map[uint64]*Consumer),
		producers: make(map[uint64]*Producer),
	}
}

// subscriptions is responsible for storing producers and consumers
// based on their IDs.
type subscriptions struct {
	cmu       sync.RWMutex // protects following
	consumers map[uint64]*Consumer

	pmu       sync.Mutex // protects following
	producers map[uint64]*Producer
}

func (s *subscriptions) addConsumer(c *Consumer) {
	s.cmu.Lock()
	s.consumers[c.consumerID] = c
	s.cmu.Unlock()
}

func (s *subscriptions) delConsumer(c *Consumer) {
	s.cmu.Lock()
	delete(s.consumers, c.consumerID)
	s.cmu.Unlock()
}

func (s *subscriptions) handleCloseConsumer(consumerID uint64, f Frame) error {
	s.cmu.Lock()
	defer s.cmu.Unlock()

	c, ok := s.consumers[consumerID]
	if !ok {
		return newErrUnexpectedMsg(f.BaseCmd.GetType(), consumerID)
	}

	delete(s.consumers, consumerID)

	return c.handleCloseConsumer(f)
}

func (s *subscriptions) handleReachedEndOfTopic(consumerID uint64, f Frame) error {
	s.cmu.Lock()
	defer s.cmu.Unlock()

	c, ok := s.consumers[consumerID]
	if !ok {
		return newErrUnexpectedMsg(f.BaseCmd.GetType(), consumerID)
	}

	return c.handleReachedEndOfTopic(f)
}

func (s *subscriptions) handleMessage(consumerID uint64, f Frame) error {
	s.cmu.RLock()
	c, ok := s.consumers[consumerID]
	s.cmu.RUnlock()

	if !ok {
		return newErrUnexpectedMsg(f.BaseCmd.GetType(), consumerID)
	}

	return c.handleMessage(f)
}

func (s *subscriptions) addProducer(p *Producer) {
	s.pmu.Lock()
	s.producers[p.producerID] = p
	s.pmu.Unlock()
}

func (s *subscriptions) delProducer(p *Producer) {
	s.pmu.Lock()
	delete(s.producers, p.producerID)
	s.pmu.Unlock()
}

func (s *subscriptions) handleCloseProducer(producerID uint64, f Frame) error {
	s.pmu.Lock()
	defer s.pmu.Unlock()

	p, ok := s.producers[producerID]
	if !ok {
		return newErrUnexpectedMsg(f.BaseCmd.GetType(), producerID)
	}

	delete(s.producers, producerID)

	return p.handleCloseProducer(f)
}
