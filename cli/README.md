cli
===

This program is a basic CLI utility that can be used to publish and subscribe to a Pulsar topic.

## Usage

```shell
$ ./cli -h
Usage of ./cli:
  -message string
    	If equal to '--', then STDIN will be used. Otherwise value with %03d $messageNumber tacked on the front will be sent (default "--")
  -name string
    	producer/consumer name (default "demo")
  -producer
    	if true, produce messages, otherwise consume
  -pulsar string
    	pulsar address (default "localhost:6650")
  -rate duration
    	rate at which to send messages (default 1s)
  -shared
    	if true, consumer is shared, otherwise exclusive
  -tls-ca string
    	(optional) path to root certificate
  -tls-cert string
    	(optional) path to TLS certificate
  -tls-insecure
    	if true, do not verify server certificate chain when using TLS
  -tls-key string
    	(optional) path to TLS key
  -topic string
    	producer/consumer topic (default "persistent://sample/standalone/ns1/demo")
```

## Build

```shell
$ go build
```

## Basic Examples

These examples connect to a Pulsar server running at `localhost:6650` (default) and use the `persistent://sample/standalone/ns1/demo` topic (default).

* Publish message repeatedly

    ```shell
    $ ./cli -producer -message "Hello" -rate 1s
    ```

* Publish from STDIN (hit enter after each message)

    ```shell
    $ ./cli -producer
    ```

* Subscribe

    ```shell
    $ ./cli -shared
    ```
