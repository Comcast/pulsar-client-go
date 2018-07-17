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

func main() {
	var args = struct {
		device string
		port   int
		dir    string
	}{
		device: "lo0",
		port:   6650,
		dir:    "corpus",
	}
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
		handlePacket(args.dir, packet)
	}
}

func handlePacket(outputDir string, pkt gopacket.Packet) {
	data := pkt.Layer(layers.LayerTypeTCP).LayerPayload()
	if len(data) == 0 {
		return
	}

	var f pulsar.Frame
	if err := f.Decode(bytes.NewReader(data)); err != nil {
		fmt.Fprintln(os.Stderr, err, data)
		return
	}

	p := path.Join(outputDir, fmt.Sprintf("%d-%s", pkt.Metadata().Timestamp.UnixNano(), f.BaseCmd.GetType().String()))
	if err := ioutil.WriteFile(p, data, os.ModePerm); err != nil {
		panic(err)
	}
	fmt.Printf("Captured %q message > %s\n", f.BaseCmd.GetType(), p)
}
