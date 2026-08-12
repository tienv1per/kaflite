package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

type Producer struct {
	port    uint16
	topicID uint16
}

func (p *Producer) sendPortDataToBroker() error {
	// client connect tới TCP server đang listen ở port 10000
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		return fmt.Errorf("connect to broker: %w", err)
	}
	defer conn.Close()
	// Buffer là vùng nhớ tạm dùng để giữ dữ liệu trước khi đọc hoặc ghi tiếp.
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	pRegMsg := ProducerRegisterMessage{
		port:    p.port,
		topicID: p.topicID,
	}
	fmt.Printf("pRegMsg: port=%d, topicID=%d\n", pRegMsg.port, pRegMsg.topicID)
	message := Message{
		PRODUCER_REGISTER: &pRegMsg,
	}
	err = writeMessageToStream(streamRW, message)
	if err != nil {
		return fmt.Errorf("send producer register error: %w", err)
	}
	// try to read back from the stream
	resp, err := readMessageFromStream(streamRW)
	if err != nil {
		return fmt.Errorf("read broker register response: %w", err)
	}
	if resp == nil || resp.R_PRODUCER_REGISTER == nil {
		return fmt.Errorf("broker returned invalid producer register response")
	}
	fmt.Printf("Received response from Broker: %v\n", *resp.R_PRODUCER_REGISTER)
	return nil
}

func (p *Producer) startProducerServer() error {
	var err error
	// Mở producer server trước để broker có thể dial ngược lại sau khi register.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", p.port))
	if err != nil {
		return fmt.Errorf("start producer server error: %w", err)
	}
	defer listener.Close()

	// Connect to Broker to send register
	err = p.sendPortDataToBroker()
	if err != nil {
		return err
	}

	// Accept connection mà Broker dial ngược lại sau bước register.
	// Từ đây về sau, mọi message stdin của Producer sẽ ghi vào conn này.
	// Ở phía Broker, conn này đang được giữ bởi goroutine riêng cho Producer port này.
	conn, err := listener.Accept()
	if err != nil {
		return fmt.Errorf("accept broker connection: %w", err)
	}
	defer conn.Close()

	// streamRW bọc đúng connection riêng giữa Producer này và Broker goroutine tương ứng.
	// Khi writeMessageToStream ghi vào streamRW, message sẽ đi qua TCP connection này
	// và được đọc bởi readMessageFromStream trong goroutine riêng phía Broker.
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
		// Gửi message lên đúng connection đã accept từ Broker.
		// Vì Broker goroutine đang đọc trên cùng connection này, message sẽ vào đúng handler đó.
		err = writeMessageToStream(streamRW, Message{PRODUCER_CONSUMER_MSG: []byte(line)})
		if err != nil {
			return err
		}
		// try to read back from stream
		resp, err := readMessageFromStream(streamRW)
		if err != nil {
			break
		}
		fmt.Printf("Received message from broker: %d\n", *resp.R_PRODUCER_CONSUMER_MSG)
	}
	err = conn.Close()
	if err != nil {
		return err
	}
	return nil
}
