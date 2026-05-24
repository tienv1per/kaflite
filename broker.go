package main

import (
	"bufio"
	"fmt"
	"net"
)

func readFromStream(stream_rw *bufio.ReadWriter) (*string, error) {
	var err error
	header, err := stream_rw.ReadByte()
	if err != nil {
		return nil, err
	}
	data, err := stream_rw.Peek(int(header))
	if err != nil {
		return nil, err
	}
	_, err = stream_rw.Discard(int(header))
	var data_str = string(data)
	return &data_str, err
}
func writeToStream(stream_rw *bufio.ReadWriter, data string) error {
	var err error
	// write
	err = stream_rw.WriteByte(byte(len(data)))
	if err != nil {
		return err
	}
	_, err = stream_rw.WriteString(data)
	if err != nil {
		return err
	}
	stream_rw.Flush()
	return nil
}

const BROKER_PORT = 10000

type Broker struct {
}

func (b *Broker) startBrokerServer() error {
	listener, _ := net.Listen("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	for {
		conn, _ := listener.Accept()
		streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		data, err := readFromStream(streamRW)
		if err != nil {
			return err
		}
		// write it back
		err = writeToStream(streamRW, *data)
		if err != nil {
			return err
		}
		err = conn.Close()
		if err != nil {
			return err
		}
	}
}
