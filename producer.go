package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

type Producer struct {
}

func (p *Producer) registerWithBroker(port int16) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		return fmt.Errorf("connect to broker: %w", err)
	}
	defer conn.Close()

	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	port_str := fmt.Sprintf("%d", port)
	message := Message{PRODUCER_REGISTER: &port_str}
	err = writeMessageToStream(streamRW, message)
	if err != nil {
		return fmt.Errorf("send producer register: %w", err)
	}
	// try to read back from the stream
	resp, err := readMessageFromStream(streamRW)
	if err != nil {
		return fmt.Errorf("read broker register response: %w", err)
	}
	if resp == nil || resp.R_PRODUCER_REGISTER == nil {
		return fmt.Errorf("broker returned invalid producer register response")
	}
	fmt.Printf("Received response from Broker: %d\n", *resp.R_PRODUCER_REGISTER)
	return nil
}

func (p *Producer) startProducerServer(port int16) error {
	// Mở producer server trước để broker có thể dial ngược lại sau khi register.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("start producer server: %w", err)
	}
	defer listener.Close()

	// Connect to Broker to send register
	err = p.registerWithBroker(port)
	if err != nil {
		return err
	}

	conn, err := listener.Accept()
	if err != nil {
		return fmt.Errorf("accept broker connection: %w", err)
	}
	defer conn.Close()

	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	reader := bufio.NewReader(os.Stdin)
	for {
		// read from stdin
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			} else {
				// panic here
				break
			}
		}
		// write message to stream ECHO
		err = writeMessageToStream(streamRW, Message{ECHO: &line})
		if err != nil {
			return err
		}
		// try to read back from stream
		resp, err := readMessageFromStream(streamRW)
		if err != nil {
			break
		}
		fmt.Printf("Received message from broker: %s\n", *resp.R_ECHO)
	}
	err = conn.Close()
	if err != nil {
		return err
	}
	return nil
}
