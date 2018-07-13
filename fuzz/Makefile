# Network device and port to use with the `corpus-gen`
# program in order to capture Pulsar packets and
# create initial fuzz corpus
CAPTURE_DEVICE := lo0
CAPTURE_PORT := 6650
# Where to saved results of fuzzing
WORKDIR := workdir

# Start fuzzing!
.PHONY: fuzz
fuzz: pulsar-fuzz.zip
	go-fuzz -bin=./pulsar-fuzz.zip -workdir=${WORKDIR}

# Create fuzz binary/zip
pulsar-fuzz.zip: ../frame_fuzz.go ../frame.go
	go-fuzz-build -o $@ $(shell go list -e ../)

# Build corpus-gen program
pulsar-corpus-gen: corpus-gen/main.go
	go build -o pulsar-corpus-gen ./corpus-gen

# Start capturing Pulsar packets in order to build
# initial corpus
.PHONY: initial-corpus
initial-corpus: pulsar-corpus-gen
	@mkdir -p workdir/corpus
	@echo "########################################################################################"
	@echo "Generate Pulsar traffic using official Pulsar client and server to create initial corpus"
	@echo "Listening for traffic on device:${CAPTURE_DEVICE} port:${CAPTURE_PORT}"
	@echo "########################################################################################"
	./pulsar-corpus-gen -device ${CAPTURE_DEVICE} -port ${CAPTURE_PORT} -dir ${WORKDIR}/corpus
