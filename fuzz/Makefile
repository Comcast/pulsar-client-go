# Copyright 2018 Comcast Cable Communications Management, LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Network device and port to use with the `corpus-gen`
# program in order to capture Pulsar packets and
# create initial fuzz corpus
CAPTURE_DEVICE := lo0
CAPTURE_PORT := 6650
INITIAL_CORPUS := initial-corpus

# Start fuzzing (Fuzz function)!
.PHONY: fuzz
fuzz: pulsar-fuzz.zip
	mkdir -p $@-workdir/corpus
	[ -d ${INITIAL_CORPUS} ] && cp ${INITIAL_CORPUS}/* $@-workdir/corpus/.
	go-fuzz -bin=./pulsar-fuzz.zip -workdir=$@-workdir

# Create fuzz binary/zip for Fuzz func
pulsar-fuzz.zip: ../frame/frame_fuzz.go ../frame/frame.go
	go-fuzz-build -func Fuzz -o $@ $(shell go list -e ../frame)

# Start fuzzing (FuzzReEncode function)!
.PHONY: fuzz-reencode
fuzz-reencode: pulsar-fuzzreencode.zip
	mkdir -p $@-workdir/corpus
	[ -d ${INITIAL_CORPUS} ] && cp ${INITIAL_CORPUS}/* $@-workdir/corpus/.
	go-fuzz -bin=./pulsar-fuzzreencode.zip -workdir=$@-workdir

# Create fuzz binary/zip for FuzzReEncode func
pulsar-fuzzreencode.zip: ../frame/frame_fuzz.go ../frame/frame.go
	go-fuzz-build -func FuzzReEncode -o $@ $(shell go list -e ../frame)

# Build corpus-gen program
pulsar-corpus-gen: corpus-gen/main.go
	go build -o pulsar-corpus-gen ./corpus-gen

# Start capturing Pulsar packets in order to build
# initial corpus
.PHONY: initial-corpus
initial-corpus: pulsar-corpus-gen
	@echo "########################################################################################"
	@echo "Generate Pulsar traffic using official Pulsar client and server to create initial corpus"
	@echo "Listening for traffic on device:${CAPTURE_DEVICE} port:${CAPTURE_PORT}"
	@echo "########################################################################################"
	@mkdir -p ${INITIAL_CORPUS}
	./pulsar-corpus-gen -device ${CAPTURE_DEVICE} -port ${CAPTURE_PORT} -dir ${INITIAL_CORPUS}
