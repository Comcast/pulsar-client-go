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

// asyncErrors provides idiom for sending in a non-blocking
// manner errors to a channel. Note: it's legal for asyncErrors
// to be nil
type asyncErrors chan<- error

// send places the error on the channel in a non-blocking way
func (a asyncErrors) send(err error) {
	// note: `a` can be nil and still work properly
	select {
	case a <- err:
	default:
	}
}
