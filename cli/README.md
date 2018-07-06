cli
===

This program is a basic CLI utility that can be used to publish and subscribe to a Pulsar topic.

## Usage

```shell
$ ./cli -h
Usage of ./cli:
  -message string
    	message to send when producing (with %03d $messageNumber tacked on the front) (default "hola mundo")
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
  -tls-cert string
    	(optional) path to TLS certificate
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

* Publish

    ```shell
    $ ./cli -producer -message "Hello" -rate 1s
    ```

* Subscribe

    ```shell
    $ ./cli -shared
    ```
