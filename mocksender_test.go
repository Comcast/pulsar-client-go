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
	"sync"

	"github.com/Comcast/pulsar-client-go/api"
)

// mockSender implements the sender interface
type mockSender struct {
	mu      sync.Mutex // protects following
	frames  []Frame
	closedc chan struct{}
}

func (m *mockSender) getFrames() []Frame {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := make([]Frame, len(m.frames))
	copy(cp, m.frames)

	return cp
}

func (m *mockSender) sendSimpleCmd(cmd api.BaseCommand) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.frames = append(m.frames, Frame{
		BaseCmd: &cmd,
	})

	return nil
}

func (m *mockSender) sendPayloadCmd(cmd api.BaseCommand, metadata api.MessageMetadata, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.frames = append(m.frames, Frame{
		BaseCmd:  &cmd,
		Metadata: &metadata,
		Payload:  payload,
	})

	return nil
}

func (m *mockSender) closed() <-chan struct{} {
	return m.closedc
}
