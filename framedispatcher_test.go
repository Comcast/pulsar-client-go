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
	"testing"
	"time"

	"github.com/Comcast/pulsar-client-go/api"
	"github.com/Comcast/pulsar-client-go/frame"
)

type dispatcherTestCase struct {
	register func() (response <-chan frame.Frame, cancel func(), err error)
	notify   func(frame frame.Frame) error
}

// dispatcherTestCases allows for the 3 types of dispatchers
// to be tested with the same tests, since they should all
// behave in the same way (register, notify)
func dispatcherTestCases() map[string]dispatcherTestCase {
	fd := newFrameDispatcher()

	return map[string]dispatcherTestCase{
		"global": {
			register: fd.registerGlobal,
			notify:   fd.notifyGlobal,
		},
		"prodSeqID": {
			register: func() (<-chan frame.Frame, func(), error) { return fd.registerProdSeqIDs(1, 2) },
			notify:   func(f frame.Frame) error { return fd.notifyProdSeqIDs(1, 2, f) },
		},
		"reqID": {
			register: func() (<-chan frame.Frame, func(), error) { return fd.registerReqID(42) },
			notify:   func(f frame.Frame) error { return fd.notifyReqID(42, f) },
		},
	}
}

func TestFrameDispatcher_Success(t *testing.T) {
	cases := dispatcherTestCases()

	for name, dtc := range cases {
		dtc := dtc
		t.Run(name, func(t *testing.T) {
			// make sure multiple register/notify calls work
			for i := 0; i < 3; i++ {
				resp, cancel, err := dtc.register()
				if err != nil {
					t.Fatalf("register() err = %v", err)
				}

				// response frame. The type and contents are arbitrary
				expected := frame.Frame{
					BaseCmd: &api.BaseCommand{
						Type: api.BaseCommand_CONNECTED.Enum(),
						Pong: &api.CommandPong{},
					},
				}

				notifyErr := make(chan error, 1)
				go func() { notifyErr <- dtc.notify(expected) }()

				select {
				case got := <-resp:
					if !got.Equal(expected) {
						t.Fatalf("got frame:\n%+v\nexpected:\n%+v", got, expected)
					}
					t.Logf("got expected frame:\n%+v", got)

					// ensure notify() returned with no errors
					select {
					case err := <-notifyErr:
						if err != nil {
							t.Fatalf("notify() err = %v; nil expected", nil)
						}
					case <-time.After(time.Second):
						t.Fatal("expected notify() to return")
					}

				case <-time.After(time.Second):
					t.Fatal("expected read from response channel")
				}

				cancel()
			}
		})
	}
}

func TestFrameDispatcher_DupRegister(t *testing.T) {
	cases := dispatcherTestCases()

	for name, dtc := range cases {
		dtc := dtc
		t.Run(name, func(t *testing.T) {
			_, _, err := dtc.register()
			if err != nil {
				t.Fatalf("register() err = %v", err)
			}

			// duplicate register
			_, _, err = dtc.register()
			if err == nil {
				t.Fatalf("duplicate register() err = %v; expected non-nil", err)
			}
			t.Logf("duplicate register() err (expected) = %v", err)
		})
	}
}

func TestFrameDispatcher_DupNotify(t *testing.T) {
	cases := dispatcherTestCases()

	for name, dtc := range cases {
		dtc := dtc
		t.Run(name, func(t *testing.T) {
			resp, cancel, err := dtc.register()
			if err != nil {
				t.Fatalf("register() err = %v", err)
			}
			defer cancel()

			// response frame. The type and contents are arbitrary
			expected := frame.Frame{
				BaseCmd: &api.BaseCommand{
					Type: api.BaseCommand_CONNECTED.Enum(),
					Pong: &api.CommandPong{},
				},
			}

			notifyErr := make(chan error, 2)
			go func() {
				for i := 0; i < 2; i++ {
					notifyErr <- dtc.notify(expected)
				}
			}()

			select {
			case <-resp:
			case <-time.After(time.Millisecond * 500):
				t.Fatal("expected read from response channel")
			}

			// first call to notify() should return nil error
			select {
			case err := <-notifyErr:
				if err != nil {
					t.Fatalf("first notify() err = %v; nil expected", err)
				}
			case <-time.After(time.Second):
				t.Fatal("expected read from notify channel")
			}

			// second call to notify() should return an error
			select {
			case err := <-notifyErr:
				if err == nil {
					t.Fatalf("second notify() err = %v; non-nil expected", err)
				}
				t.Logf("second notify() err (expected) = %v", err)
			case <-time.After(time.Second):
				t.Fatal("expected read from notify channel")
			}
		})
	}
}

func TestFrameDispatcher_Unexpected(t *testing.T) {
	cases := dispatcherTestCases()

	for name, dtc := range cases {
		dtc := dtc
		t.Run(name, func(t *testing.T) {
			// no register is called

			// response frame. The type and contents are arbitrary
			f := frame.Frame{
				BaseCmd: &api.BaseCommand{
					Type: api.BaseCommand_CONNECTED.Enum(),
					Pong: &api.CommandPong{},
				},
			}

			err := dtc.notify(f)
			if err == nil {
				t.Fatalf("notify() err = %v; expected non-nil", err)
			}
			t.Logf("notify() (expected) err = %+v", err)
		})
	}
}

func TestFrameDispatcher_Timeout(t *testing.T) {
	cases := dispatcherTestCases()

	for name, dtc := range cases {
		dtc := dtc
		t.Run(name, func(t *testing.T) {
			resp, cancel, err := dtc.register()
			if err != nil {
				t.Fatalf("register() err = %v", err)
			}

			// The side that waits for the response
			// is expected to call cancel after either
			// receiving a response or timing out.
			cancel()

			// response frame. The type and contents are arbitrary
			f := frame.Frame{
				BaseCmd: &api.BaseCommand{
					Type: api.BaseCommand_CONNECTED.Enum(),
					Pong: &api.CommandPong{},
				},
			}

			err = dtc.notify(f)
			if err == nil {
				t.Fatalf("notify() err = %v; expected non-nil", err)
			}
			t.Logf("notify() (expected) err = %+v", err)

			// read from response channel is NOT expected
			select {
			case got := <-resp:
				t.Fatalf("got unexpected expected frame:\n%+v", got)

			default:
				t.Log("response channel blocked (expected)")
			}
		})
	}
}
