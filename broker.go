package main

import (
	"bufio"
	"fmt"
	"net"
)

const BROKER_PORT = 10000
const (
	ECHO = 1
	// other message types
)

type Message struct {
	// Nếu ECHO != nil thì message này là echo request
	ECHO *string
}

func readFromStream(stream_rw *bufio.ReadWriter) ([]byte, error) {
	var err error
	// Protocol: [length][data]
	header, err := stream_rw.ReadByte()
	if err != nil {
		return nil, err
	}
	// Đọc đúng số byte mà header báo
	data, err := stream_rw.Peek(int(header))
	if err != nil {
		return nil, err
	}
	// Bỏ data đã đọc ra khỏi buffer
	_, err = stream_rw.Discard(int(header))
	return data, err
}

func writeToStream(stream_rw *bufio.ReadWriter, data string) error {
	var err error
	// Gửi response theo protocol: [length][data]
	err = stream_rw.WriteByte(byte(len(data)))
	if err != nil {
		return err
	}
	// Ghi nội dung response
	_, err = stream_rw.WriteString(data)
	if err != nil {
		return err
	}
	// Đẩy buffer ra TCP connection
	stream_rw.Flush()
	return nil
}

type Broker struct {
}

func (b *Broker) startBrokerServer() error {
	// Mở TCP server tại BROKER_PORT
	listener, _ := net.Listen("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	for {
		// Chờ client kết nối
		conn, _ := listener.Accept()
		streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		// Đọc raw message từ TCP stream
		data, err := readFromStream(streamRW)
		if err != nil {
			return err
		}
		// Parse raw message thành Message theo type
		parsed_message := b.parseBrokerMessage(data)
		if parsed_message != nil {
			// Xử lý message và tạo response
			resp, err := b.processBrokerMessage(parsed_message)
			if err != nil {
				fmt.Print("error1", err)
				return err
			}
			// Gửi response về client
			err = writeToStream(streamRW, resp)
			if err != nil {
				fmt.Print("error2", err)
				return err
			}
		}
		err = conn.Close()
		if err != nil {
			return err
		}
	}
}

func (b *Broker) parseBrokerMessage(message []byte) *Message {
	// Byte đầu tiên là message type
	switch message[0] {
	case ECHO:
		// Các byte còn lại là nội dung echo
		var st = string(message[1:])
		return &Message{ECHO: &st}
	default:
		return nil
	}
}

func (b *Broker) processBrokerMessage(message *Message) (string, error) {
	var err error
	var resp string
	if message.ECHO != nil {
		// Echo response cho client
		resp = fmt.Sprintf("I have receiver: %s", *message.ECHO)
	}
	return resp, err
}
