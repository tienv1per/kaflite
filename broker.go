package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
)

const BROKER_PORT = 10000

type Broker struct {
}

// startBrokerServer mở TCP listener và xử lý request từ client.
// Input: receiver Broker, dùng để gọi các hàm xử lý message.
// Output: trả error nếu listen/read/process/write/close gặp lỗi.
// Cần hàm này để broker nhận connection, đọc message, xử lý và gửi response về client.
func (b *Broker) startBrokerServer() error {
	// Mở TCP server tại BROKER_PORT
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		// Chờ client kết nối
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		// Đọc raw message từ TCP stream
		message, err := readMessageFromStream(streamRW)
		fmt.Println("Got some message from client...")
		if err != nil {
			return err
		}
		if message != nil {
			// Xử lý message và tạo response
			resp, err := b.processBrokerMessage(message)
			if err != nil {
				fmt.Print("error1", err)
				return err
			}
			if resp == nil {
				_ = conn.Close()
				continue
			}
			// Gửi response về client
			err = writeMessageToStream(streamRW, *resp)
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

// processBrokerMessage route message tới handler đúng theo loại message.
// Input: message đã được parse từ TCP stream.
// Output: response Message tương ứng hoặc nil nếu message type chưa được hỗ trợ.
// Cần hàm này để tách logic xử lý từng loại request khỏi logic network của broker.
func (b *Broker) processBrokerMessage(message *Message) (*Message, error) {
	if message.ECHO != nil {
		// Echo response cho client
		resp, err := b.processEchoMessage(message.ECHO)
		if err != nil {
			return nil, err
		}
		return &Message{R_ECHO: &resp}, nil
	}
	if message.PRODUCER_REGISTER != nil {
		// Echo response cho client
		resp, err := b.processProducerRegisterMessage(message.PRODUCER_REGISTER)
		if err != nil {
			return nil, err
		}
		return &Message{R_PRODUCER_REGISTER: resp}, nil
	}
	return nil, nil
}

// processEchoMessage xử lý ECHO request.
// Input: message là nội dung client gửi lên.
// Output: chuỗi response xác nhận broker đã nhận message, kèm error nếu sau này có lỗi xử lý.
// Cần hàm này để đóng gói riêng logic ECHO và dễ mở rộng thêm message type khác.
func (b *Broker) processEchoMessage(message *string) (string, error) {
	return fmt.Sprintf("I have receiver: %s", *message), nil
}

func (b *Broker) processProducerRegisterMessage(p_register_message *string) (*byte, error) {
	port, err := strconv.ParseInt(*p_register_message, 10, 16)
	if err != nil {
		return nil, err
	}
	go func() {
		conn, err := net.Dial("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			fmt.Printf("Error connecting to producer server at port %v: %v\n", port, err)
			return
		}
		defer conn.Close()

		fmt.Printf("Connected to producer server at port: %v\n", port)
		// read input from stdin and write to stream
		streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		for {
			message, err := readMessageFromStream(streamRW)
			if message == nil || err != nil {
				fmt.Printf("Producer connection at port %v closed or invalid: %v\n", port, err)
				return
			}
			// process something here
			resp, err := b.processBrokerMessage(message)
			if err != nil {
				fmt.Printf("Error processing producer message from port %v: %v\n", port, err)
				return
			}
			if resp == nil {
				fmt.Printf("Producer message from port %v has no response\n", port)
				continue
			}
			err = writeMessageToStream(streamRW, *resp)
			if err != nil {
				fmt.Printf("Error writing producer response to port %v: %v\n", port, err)
				return
			}
		}
	}()
	var resp byte = 0
	return &resp, nil
}
