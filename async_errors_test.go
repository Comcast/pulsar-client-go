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
	"fmt"
	"testing"
)

func TestAsyncErrors(t *testing.T) {
	c := make(chan error, 2)
	a := asyncErrors(c)

	// test twice channel's capacity to ensure
	// report doesn't block
	for i := 0; i < cap(c)*2; i++ {
		a.send(fmt.Errorf("error %d", i))
	}

	for i := 0; i < cap(c); i++ {
		select {
		case got := <-c:
			t.Log(got)
		default:
			t.Fatalf("expected %d read from error channel; blocked", i)
		}
	}
}

func TestAsyncErrors_Nil(t *testing.T) {
	a := asyncErrors(nil)

	for i := 0; i < 2; i++ {
		a.send(fmt.Errorf("error %d", i))
	}
}
