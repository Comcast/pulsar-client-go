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
	"sort"
	"sync"
	"testing"
)

func TestMonotonicIDs(t *testing.T) {
	start := uint64(42)
	rid := monotonicID{start}

	for i := 0; i < 100; i++ {
		expected := start + uint64(i)
		got := rid.next()
		if *got != expected {
			t.Fatalf("(%d) monotonicID{%d}.next() = %d; expected %d", i, start, *got, expected)
		} else {
			t.Logf("(%d) monotonicID{%d}.next() = %d", i, start, *got)
		}
	}
}

func TestMonotonicIDs_Parallel(t *testing.T) {
	first := 42
	rid := monotonicID{uint64(first)}

	expected := make([]int, 100)
	var mu sync.Mutex
	got := make([]int, 0, 100)

	start := make(chan struct{})
	var done sync.WaitGroup

	for i := range expected {
		expected[i] = first + i

		done.Add(1)
		go func() {
			defer done.Done()
			<-start

			id := rid.next()

			mu.Lock()
			got = append(got, int(*id))
			mu.Unlock()
		}()
	}

	close(start)
	done.Wait()

	sort.Ints(got)

	if gotl, expectedl := len(got), len(expected); gotl != expectedl {
		t.Fatalf("got %d elements; expected %d", gotl, expectedl)
	}

	for i := range expected {
		if goti, expectedi := got[i], expected[i]; goti != expectedi {
			t.Fatalf("got element %d = %d; expected %d", i, goti, expectedi)
		}
	}
}
