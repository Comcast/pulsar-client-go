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
pulsar-fuzz.zip: ../frame_fuzz.go ../frame.go
	go-fuzz-build -func Fuzz -o $@ $(shell go list -e ../)

# Start fuzzing (FuzzReEncode function)!
.PHONY: fuzz-reencode
fuzz-reencode: pulsar-fuzzreencode.zip
	mkdir -p $@-workdir/corpus
	[ -d ${INITIAL_CORPUS} ] && cp ${INITIAL_CORPUS}/* $@-workdir/corpus/.
	go-fuzz -bin=./pulsar-fuzzreencode.zip -workdir=$@-workdir

# Create fuzz binary/zip for FuzzReEncode func
pulsar-fuzzreencode.zip: ../frame_fuzz.go ../frame.go
	go-fuzz-build -func FuzzReEncode -o $@ $(shell go list -e ../)

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
