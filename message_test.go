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

import "testing"

func TestMessage_Ack(t *testing.T) {
	var ms mockSender

	m := Message{
		s: &ms,
	}

	if err := m.Ack(); err != nil {
		t.Fatalf("Ack() err = %v; expected nil", err)
	}

	if got, expected := len(ms.frames), 1; got != expected {
		t.Fatalf("got %d frame; expected %d", got, expected)
	}
}
