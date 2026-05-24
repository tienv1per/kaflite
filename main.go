package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	if os.Args[1] == "server" {
		spawnServer()
	} else {
		clientConnect(os.Args[2])
	}
}

func spawnServer() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		startServer("10001")
	}()
	go func() {
		defer wg.Done()
		startServer("10002")
	}()
	wg.Wait()
}

func startServer(port string) {
	ln, _ := net.Listen("tcp", fmt.Sprintf(":%s", port))
	conn, _ := ln.Accept() // Chờ client kết nối
	stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	for {
		// Protocol: [1 byte độ dài][n byte dữ liệu]
		header, _ := stream_rw.ReadByte()      // Đọc độ dài message, block nếu chưa có dữ liệu
		data, _ := stream_rw.Peek(int(header)) // Nhìn n byte tiếp theo, chưa lấy khỏi buffer
		fmt.Printf("Data from client: %s\n", data)
		if strings.Trim(string(data), "\n ") == "bye" {
			break
		}
		stream_rw.Discard(int(header))      // Bỏ data đã đọc ra khỏi buffer
		time.Sleep(2000 * time.Millisecond) // Giả lập xử lý chậm

		newData := fmt.Sprintf("Received from client: %s", string(data))
		stream_rw.WriteByte(byte(len(newData))) // Header đang dùng độ dài data gốc
		stream_rw.WriteString(newData)       // Ghi response vào buffer
		stream_rw.Flush()                    // Đẩy buffer ra TCP connection
	}
	conn.Close()
}

func clientConnect(port string) {
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%s", port))
	for {
		// Đọc 1 dòng người dùng nhập từ terminal
		rd := bufio.NewReader(os.Stdin)
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		fmt.Printf("Send to server: %s\n", line)

		stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		// Gửi theo protocol: [1 byte độ dài][n byte dữ liệu]
		stream_rw.WriteByte(byte(len(line))) // Gửi độ dài message
		stream_rw.WriteString(line)          // Gửi nội dung message
		stream_rw.Flush()                    // Đẩy buffer ra TCP connection
		if strings.Trim(line, "\n ") == "bye" {
			break
		}

		// Đọc response từ server theo cùng protocol
		header, _ := stream_rw.ReadByte()      // Đọc độ dài response
		data, _ := stream_rw.Peek(int(header)) // Nhìn n byte tiếp theo, chưa lấy khỏi buffer
		fmt.Printf("Data from server: %s\n", data)
		stream_rw.Discard(int(header)) // Bỏ response đã đọc ra khỏi buffer
	}

	conn.Close()
}
