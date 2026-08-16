package main

import (
	"bufio"
	"fmt"
	"net"
)

type Consumer struct {
	port    uint16
	topicID uint16
	groupID uint16
}

func (c *Consumer) sendPortDataToBroker() error {
	// client connect tới TCP server đang listen ở port 10000
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		return fmt.Errorf("connect to broker: %w", err)
	}
	defer conn.Close()
	// Buffer là vùng nhớ tạm dùng để giữ dữ liệu trước khi đọc hoặc ghi tiếp.
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	cRegMsg := ConsumerRegisterMessage{
		port:    c.port,
		topicID: c.topicID,
		groupID: c.groupID,
	}
	fmt.Printf("cRegMsg: port=%d, topicID=%d, groupID=%d\n", cRegMsg.port, cRegMsg.topicID, cRegMsg.groupID)
	message := Message{
		CONSUMER_REGISTER: &cRegMsg,
	}
	err = writeMessageToStream(streamRW, message)
	if err != nil {
		return fmt.Errorf("send consumer register error: %w", err)
	}
	// try to read back from the stream
	resp, err := readMessageFromStream(streamRW)
	if err != nil {
		return fmt.Errorf("read broker register response: %w", err)
	}
	if resp == nil || resp.R_CONSUMER_REGISTER == nil {
		return fmt.Errorf("broker returned invalid consumer register response")
	}
	fmt.Printf("Consumer received response from Broker: %v\n", *resp.R_CONSUMER_REGISTER)
	return nil
}

func (c *Consumer) startConsumerServer() error {
	var err error
	// Mở consumer server trước để broker có thể dial ngược lại sau khi register.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", c.port))
	if err != nil {
		return fmt.Errorf("start consumer server error: %w", err)
	}
	defer listener.Close()

	// Connect tới Broker để gửi thông tin register: consumer port, topicID và groupID.
	// Sau bước này, Broker biết Consumer thuộc group nào và sẽ dial ngược lại c.port.
	err = c.sendPortDataToBroker()
	if err != nil {
		return err
	}

	// Accept connection mà Broker dial ngược lại sau bước register.
	// Từ đây về sau, Broker sẽ dùng conn này để push message từ consumer group xuống Consumer.
	conn, err := listener.Accept()
	if err != nil {
		return fmt.Errorf("accept broker connection: %w", err)
	}
	defer conn.Close()

	// streamRW bọc đúng connection riêng giữa Consumer này và Broker.
	// readMessageFromStream sẽ block tới khi Broker gửi data message qua connection này.
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	
	fmt.Printf("Start consumer server, receiving...\n")
	for {
		// Chờ Broker gửi message cần consume.
		// Hiện tại Broker dùng lại PRODUCER_CONSUMER_MSG để mang payload từ topic queue xuống Consumer.
		message, err := readMessageFromStream(streamRW)
		if err != nil {
			break
		}
		fmt.Printf("Received message PCM from broker: %s\n", message.PRODUCER_CONSUMER_MSG)

		// Gửi ACK/status ngược lại để Broker biết Consumer đã nhận/xử lý message này.
		// Broker sẽ dùng response này để đánh dấu Consumer ready cho message tiếp theo.
		var resp byte = 1
		err = writeMessageToStream(streamRW, Message{
			R_PRODUCER_CONSUMER_MSG: &resp,
		})
		if err != nil {
			break
		}
	}
	err = conn.Close()
	if err != nil {
		return err
	}
	return nil
}
