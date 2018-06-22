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
	"flag"
	"testing"
)

// pulsarAddr, if provided, is the Pulsar server to use for integration
// tests (most likely Pulsar standalone running on localhost).
var _pulsarAddr = flag.String("pulsar", "", "Address of Pulsar server to connect to. If blank, tests are skipped")

// pulsarAddr is a helper function that either returns the non-blank
// pulsar address, or skips the test.
func pulsarAddr(t *testing.T) string {
	v := *_pulsarAddr
	if v != "" {
		return v
	}
	t.Skip("pulsar address (-pulsar) not provided")
	return ""
}
