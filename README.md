pulsar-client-go
=========

A Go client library for the [Apache Pulsar](https://pulsar.incubator.apache.org/) project.

[![GoDoc](https://godoc.org/github.com/Comcast/pulsar-client-go?status.svg)](https://godoc.org/github.com/Comcast/pulsar-client-go)

## Alternatives

The Pulsar project contains a [Go client library](https://pulsar.incubator.apache.org/docs/latest/clients/go/) that is a wrapper for the Pulsar C++ client library.

In comparison, this library is 100% Go (no cgo required). Outside the Go standard library, it has a single dependency on the [`golang/protbuf`](/Gopkg.toml) library.

## Status & Goals

**Status**

This client is a work-in-progress and as such does not support all Pulsar features. It supports `v1.22` of Pulsar.

The following is an incomplete list of features that are not yet implemented:

* Batch frame support
* Payload compression support
* Partitioned topics support
* Athenz authentication support
* Encryption support
* Pulsar `2.x` support

**Goals**

* 100% Go
* Simplicity

## Installation

```shell
go get -u github.com/Comcast/pulsar-client-go
```

_Note: The package name is `pulsar`_

## Example

An example of a producer and consumer can be seen in the included [cli](https://github.com/Comcast/pulsar-client-go/blob/master/cli/main.go) application.

## Contributions

Contributions are welcome. Please create an issue before beginning work on major contributions. Refer to the [CONTRIBUTING.md](/CONTRIBUTING.md) doc for more information.

# Local Development

## Integration Tests

Integration tests are provided that connect to a Pulsar sever.
They are best run against a local instance of Pulsar, since they expect the standalone properties to exist.
See below for instructions on installing Pulsar locally.

Integration tests will be run when provided the `pulsar` flag with the address of the Pulsar server to connect to. Example:

    go test -v -pulsar "localhost:6650"

## Protobuf

The `Makefile` target `api/PulsarApi.pb.go` will generate the required .go files
using the Pulsar source's .proto files.

Usage:

```shell
$ make api/PulsarApi.pb.go
```

## Local Pulsar

Notes on installing Pulsar locally.

Prereqs:

 * For Java8 on OSX, use these instructions [stackoverflow](https://stackoverflow.com/questions/24342886/how-to-install-java-8-on-mac)
 * Checkout source from github

	```shell
	git clone git@github.com:apache/incubator-pulsar.git
	```

 * Switch to desired tag, eg `v1.22.1-incubating`
 * Install Maven

	```shell
	brew install maven
	```

 * Compile ([full instructions](https://github.com/apache/incubator-pulsar#build-pulsar))

	```shell
	mvn install -DskipTests
	```

Launch Pulsar from Pulsar directory:

```shell
./bin/pulsar standalone --wipe-data --advertised-address localhost
```

## Local Pulsar + TLS

The `Makefile` has various targets to support certificate generation, Pulsar TLS configuration, and topic setup:

* Generate certificates for use by brokers, admin tool, and applications:

	```shell
	make certificates
	```

	This will create `broker`, `admin`, and `app` private/public pairs in the certs directory.

* Generate configuration files for running Pulsar standalone and `pulsar-admin` with TLS enabled using generated certificates:

	```shell
	make pulsar-tls-conf
	```

	This will generate `pulsar-conf/standalone.tls.conf` and `pulsar-conf/client.tls.conf` files that can be used as the configurations
	for the standalone server and `pulsar-admin` tools respectively. They'll use the certificates in the `certs` directory. The files should
	be placed in the appropriate locations for use with those tools (probably the `conf` directory within the Pulsar directory). It's recommended
	to use symbolic-links to easily switch between configurations.

* Setup sample topic on standalone server with TLS enabled:

	```shell
	make standalone-tls-ns
	```

	This will create a `sample/standalone/ns1` topic. The `app` certificate will have `produce`, `consume` rights on the topic.

# License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
