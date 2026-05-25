package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func main() {
	if os.Args[1] == "server" {
		// Chạy broker TCP server
		var broker = Broker{}
		err := broker.startBrokerServer()
		if err != nil {
			fmt.Printf("Error starting broker: %v\n", err.Error())
		}
	} else {
		// Chạy client và gửi message ECHO tới broker
		clientConnectTCPAndEcho(BROKER_PORT)
	}
}

func writeEchoToStream(streamRW *bufio.ReadWriter, data string) error {
	var err error
	// Protocol request: [length][type][data]
	err = streamRW.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}
	// Type ECHO cho broker biết đây là message echo
	err = streamRW.WriteByte(ECHO)
	if err != nil {
		return err
	}
	// Ghi nội dung message sau type
	_, err = streamRW.WriteString(data)
	if err != nil {
		return err
	}
	// Đẩy toàn bộ buffer ra TCP connection
	err = streamRW.Flush()
	if err != nil {
		return err
	}
	return nil
}

func clientConnectTCPAndEcho(port int) {
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", port))
	fmt.Printf("Connected to server at port %v\n", port)

	// Đọc 1 dòng từ terminal
	reader := bufio.NewReader(os.Stdin)
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	line, err := reader.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return
		} else {
			// panic here
		}
	}
	fmt.Printf("Sent to server: %s\n", line)
	writeEchoToStream(streamRW, strings.Trim(line, "\n"))

	// Đọc response từ broker theo protocol: [length][data]
	header, err := streamRW.ReadByte()
	if header == 0 || err != nil {
		return
	}
	// Peek chỉ nhìn data, chưa bỏ khỏi buffer
	data, _ := streamRW.Peek(int(header))
	fmt.Printf("Receive message from server: %s\n", data)
	// Bỏ response đã đọc ra khỏi buffer
	streamRW.Discard(int(header))
}
