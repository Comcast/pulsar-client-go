fuzz
====

The [`Frame.Decode(r io.Reader)`](../frame.go) method decodes data from a TCP connection and
converts it into a Pulsar frame. Since it accepts traffic from the network, it a good candidate for fuzz testing.

From the `go-fuzz` README:
> Fuzzing is mainly applicable to packages that parse complex inputs (both text and binary),
> and is especially useful for hardening of systems that parse inputs from potentially
> malicious users (e.g. anything accepted over a network).

The [`frame_fuzz.go`](../frame_fuzz.go) file contains the two entrypoints to the fuzzer.

## Prerequisites

* The https://github.com/dvyukov/go-fuzz application is used to perform the fuzzing. The project's README includes
detailed installation instructions.

    ```shell
    $ go get -u github.com/dvyukov/go-fuzz/...
    ```

* For the initial corpus generator, the github.com/google/gopacket library is required. Installation:

    ```shell
    $ go get -u github.com/google/gopacket/...
    ```

## Initial Input Corpus

An initial _corpus_ is a collection of valid Pulsar frames that the fuzzer will use as a starting point for its input.

From the `go-fuzz` project:
> Ideally, files in the corpus are as small as possible and as diverse as possible.
> You can use inputs used by unit tests and/or generate them.
> For example, for an image decoding package you can encode several small bitmaps
> (black, random noise, white with few non-white pixels) with different levels of
> compressions and use that as the initial corpus. Go-fuzz will deduplicate and
> minimize the inputs. So throwing in a thousand of inputs is fine, diversity is more important.

The `/fuzz` directory includes an application and Makefile to help generate the initial corpus
by capturing Pulsar traffic from the network and recording Pulsar packets to disk.

**Capturing Packets**

* To build the packet capturer program and run it:

    ```shell
    $ make initial-corpus
    ```

* While the packet capturer is running, generate Pulsar traffic on the listening network device. For example,
the standalone server can be used along with the Java Pulsar client to publish messages and subscribe to topics.
As many unique message types as possible should be created.

* The Pulsar packets will be saved to the `/fuzz/initial-corpus` directory as they're captured. The `fuzz` and `fuzz-reencode` make targets
will copy over the initial corpus files to specific _workdir_ directories for each test.

## Fuzzing

The fuzzer binary can be created/updated and fuzzing started with the following commands:

    ```shell
    make fuzz
    ```

    ```shell
    make fuzz-reencode
    ```

The fuzzer will run indefinitely (and use a lot of CPU). Interesting results will be saved in the `/fuzz/{fuzz,fuzz-reencode}-workdir/crashers` directories.
