package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

// main là entrypoint của chương trình.
// Input: os.Args[1] để quyết định chạy server hay client.
// Output: không return giá trị; nó start broker server hoặc chạy client gửi message.
// Cần hàm này để gom logic chọn mode chạy từ command line.
func main() {
	switch os.Args[1] {
	case "server":
		fmt.Println("Trying to start broker process")
		// Chạy broker TCP server
		var broker = Broker{}
		broker.init()
		err := broker.startBrokerServer()
		if err != nil {
			fmt.Printf("Error starting broker: %v\n", err.Error())
		}
	case "producer":
		fmt.Println("Trying to start producer process")
		port, err := strconv.ParseInt(os.Args[2], 10, 16)
		if err != nil {
			panic(err)
		}
		topicID, err := strconv.ParseInt(os.Args[3], 10, 16)
		if err != nil {
			panic(err)
		}
		var producer = Producer{
			port:    uint16(port),
			topicID: uint16(topicID),
		}
		err = producer.startProducerServer()
		if err != nil {
			fmt.Printf("Error starting producer: %v\n", err.Error())
		}
	case "consumer":
		fmt.Println("Trying to start consumer process")
		port, err := strconv.ParseInt(os.Args[2], 10, 16)
		if err != nil {
			panic(err)
		}
		topicID, err := strconv.ParseInt(os.Args[3], 10, 16)
		if err != nil {
			panic(err)
		}
		groupID, err := strconv.ParseInt(os.Args[4], 10, 16)
		if err != nil {
			panic(err)
		}
		var consumer = Consumer{
			port:    uint16(port),
			topicID: uint16(topicID),
			groupID: uint16(groupID),
		}
		err = consumer.startConsumerServer()
		if err != nil {
			fmt.Printf("Error starting consumer: %v\n", err.Error())
		}
	default:
		// Chạy client và gửi message ECHO tới broker
		clientConnectTCPAndEcho(BROKER_PORT)
	}
}

// clientConnectTCPAndEcho tạo TCP client kết nối tới broker và gửi một ECHO message.
// Input: port là cổng TCP của broker server.
// Output: không return giá trị; nó in response từ broker ra terminal hoặc panic nếu read/write lỗi.
// Cần hàm này để test luồng request-response giữa client và broker qua TCP.
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
	message := Message{ECHO: &line}
	err = writeMessageToStream(streamRW, message)
	if err != nil {
		panic(err)
	}

	// Đọc response từ broker theo protocol: [length][data]
	resp, err := readMessageFromStream(streamRW)
	if err != nil {
		panic(err)
	}
	// Peek chỉ nhìn data, chưa bỏ khỏi buffer
	fmt.Printf("Receive message from server: %s\n", *resp.R_ECHO)
}
