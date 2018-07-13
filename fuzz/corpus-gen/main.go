package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	pulsar "github.com/Comcast/pulsar-client-go"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
)

var args = struct {
	device string
	port   int
	dir    string
}{
	device: "lo0",
	port:   6650,
	dir:    "corpus",
}

func main() {
	flag.StringVar(&args.device, "device", args.device, "device to capture packets from")
	flag.IntVar(&args.port, "port", args.port, "port to capture packets on")
	flag.StringVar(&args.dir, "dir", args.dir, "directory to store corpus files")
	flag.Parse()

	handle, err := pcap.OpenLive(args.device, 1600, true, pcap.BlockForever)
	if err != nil {
		panic(err)
	}
	defer handle.Close()
	if err := handle.SetBPFFilter(fmt.Sprintf("tcp and port %d", args.port)); err != nil {
		panic(err)
	}

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	for packet := range packetSource.Packets() {
		handlePacket(packet)
	}
}

func handlePacket(pkt gopacket.Packet) {
	data := pkt.Layer(layers.LayerTypeTCP).LayerPayload()
	if len(data) == 0 {
		return
	}

	var f pulsar.Frame
	if err := f.Decode(bytes.NewReader(data)); err != nil {
		fmt.Fprintln(os.Stderr, err, data)
		return
	}

	p := path.Join(args.dir, fmt.Sprintf("%d-%s", pkt.Metadata().Timestamp.UnixNano(), f.BaseCmd.GetType().String()))
	if err := ioutil.WriteFile(p, data, os.ModePerm); err != nil {
		panic(err)
	}
	fmt.Printf("Captured %q message > %s\n", f.BaseCmd.GetType(), p)
}
