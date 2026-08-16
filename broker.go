package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

const BROKER_PORT = 10000

type Broker struct {
	topics []Topic
}

func (b *Broker) init() {
	b.topics = make([]Topic, 0)
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
		if message == nil || err != nil {
			return err
		}
		// Xử lý message và tạo response
		resp, err := b.processBrokerMessage(message)
		if err != nil {
			return err
		}
		if resp == nil {
			_ = conn.Close()
			continue
		}
		// Gửi response về client
		err = writeMessageToStream(streamRW, *resp)
		if err != nil {
			return err
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
		resp, err := b.processProducerRegisterMessage(*message.PRODUCER_REGISTER)
		if err != nil {
			return nil, err
		}
		return &Message{R_PRODUCER_REGISTER: resp}, nil
	}
	if message.CONSUMER_REGISTER != nil {
		resp, err := b.processConsumerRegisterMessage(*message.CONSUMER_REGISTER)
		if err != nil {
			return nil, err
		}
		return &Message{R_CONSUMER_REGISTER: resp}, nil
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

// processProducerRegisterMessage xử lý request đăng ký Producer với Broker.
// Input: p_register_message là port Producer đang listen để Broker có thể dial ngược lại.
// Output: trả byte response đăng ký thành công, hoặc error nếu port không hợp lệ.
// Cần hàm này để sau bước register, Broker tạo goroutine riêng giữ connection với Producer và đọc nhiều message tiếp theo.
func (b *Broker) processProducerRegisterMessage(p_register_message ProducerRegisterMessage) (*byte, error) {
	fmt.Printf("Broker received pRegMessage: port=%d, topicID=%d\n", p_register_message.port, p_register_message.topicID)
	// Tạo goroutine riêng cho Producer vừa register.
	// Goroutine này capture port của Producer, dial vào đúng Producer đó,
	// rồi giữ conn riêng để đọc mọi message Producer gửi về sau trên connection này.
	var topicIdx int = -1
	for idx, topic := range b.topics {
		if topic.topicID == p_register_message.topicID {
			topicIdx = idx
			break
		}
	}
	if topicIdx == -1 {
		topic := Topic{}
		topic.init(p_register_message.topicID)
		b.topics = append(b.topics, topic)
		topicIdx = len(b.topics) - 1
		go b.stopAndPop(uint(topicIdx))
	}

	go func() {
		// conn này là TCP connection riêng giữa Broker goroutine này và Producer port tương ứng.
		// Producer ghi message vào chính connection này, nên OS/TCP sẽ đưa bytes tới goroutine
		// đang block ở readMessageFromStream bên dưới. Không cần router thủ công trong code.
		conn, err := net.Dial("tcp", fmt.Sprintf(":%d", p_register_message.port))
		if err != nil {
			fmt.Printf("Error connecting to producer server at port %v: %v\n", p_register_message.port, err)
			return
		}
		defer conn.Close()

		fmt.Printf("Connected to producer server at port: %v\n", p_register_message.port)
		// streamRW bọc đúng conn riêng của Producer này.
		// Read từ streamRW chỉ nhận data đi qua connection này, không lẫn với Producer khác.
		streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		for {
			// Mỗi lần Producer gửi message tiếp theo trên cùng TCP connection,
			// goroutine này sẽ được đánh thức để đọc frame [length][message_type][payload].
			message, err := readMessageFromStream(streamRW)
			if message == nil || err != nil {
				fmt.Printf("Producer connection at port %v closed or invalid: %v\n", p_register_message.port, err)
				return
			}
			// Nếu Producer gửi data message, Broker sẽ lưu message đó vào queue của topic
			// mà Producer đã đăng ký trước đó. topicIdx được tìm/tạo ở bước register.
			if message.PRODUCER_CONSUMER_MSG != nil {
				// processProducerConsumerMessage push payload vào message queue của topic tương ứng.
				resp, err := b.processProducerConsumerMessage(message.PRODUCER_CONSUMER_MSG, uint16(topicIdx))
				if err != nil {
					panic(err)
				}
				// Gửi ACK/status ngược lại để Producer biết Broker đã xử lý message này.
				err = writeMessageToStream(streamRW, Message{R_PRODUCER_CONSUMER_MSG: &resp})
				if err != nil {
					panic(err)
				}
			}
		}
	}()
	var resp byte = 0
	return &resp, nil
}

func (b *Broker) processProducerConsumerMessage(producer_consumer_msg []byte, topicIdx uint16) (byte, error) {
	b.topics[topicIdx].mq.push(producer_consumer_msg)
	b.topics[topicIdx].mq.debug()
	return 0, nil
}

// processConsumerRegisterMessage xử lý request đăng ký Consumer với Broker.
// Input: cRegMessage gồm port Consumer, topicID cần đọc, và groupID của consumer group.
// Output: trả byte response đăng ký thành công, hoặc error nếu sau này có lỗi xử lý.
// Cần hàm này để Broker gắn Consumer vào đúng topic/group và tạo loop gửi message cho group đó.
func (b *Broker) processConsumerRegisterMessage(cRegMessage ConsumerRegisterMessage) (*byte, error) {
	fmt.Printf("Broker received cRegMessage: port=%d, topicID=%d, groupID=%d\n", cRegMessage.port, cRegMessage.topicID, cRegMessage.groupID)
	// Tìm hoặc tạo topic mà Consumer muốn đọc.
	var topicIdx int = -1
	for _, tp := range b.topics {
		if tp.topicID == cRegMessage.topicID {
			topicIdx = int(tp.topicID)
			break
		}
	}

	if topicIdx == -1 {
		// Topic mới cần init để có queue riêng và danh sách consumer group rỗng.
		topic := Topic{}
		topic.init(cRegMessage.topicID)
		b.topics = append(b.topics, topic)
		topicIdx = len(b.topics) - 1
	}

	// Tìm hoặc tạo consumer group trong topic; mỗi group có offset đọc riêng.
	// Hai group khác nhau có thể đọc cùng topic ở hai tốc độ khác nhau.
	var groupIdx int = -1
	for idx, cgroup := range b.topics[topicIdx].cgroups {
		if cgroup.groupID == cRegMessage.groupID {
			groupIdx = idx
			break
		}
	}

	if groupIdx == -1 {
		// Group mới bắt đầu từ offset 0, tức là đọc từ message đầu tiên của topic queue.
		cgroup := CGroup{
			groupID: cRegMessage.groupID,
			offset:  0,
		}
		b.topics[topicIdx].cgroups = append(b.topics[topicIdx].cgroups, cgroup)
		groupIdx = len(b.topics[topicIdx].cgroups) - 1

		// Start consumption loop một lần cho consumer group mới.
		// Các Consumer đăng ký thêm cùng group sẽ dùng chung loop này.
		// Hiện tại design là 1 consumer group có 1 goroutine đọc queue theo offset của group,
		// chưa phải mỗi ConsumerConn chạy trong một goroutine riêng.
		go b.startConsumerGroupConsumption(uint(topicIdx), uint(groupIdx))
	}

	// Dial tới Consumer port và lưu connection để Broker có thể push message về Consumer.
	// Flow này giống Producer register: client báo port, Broker chủ động dial ngược lại.
	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", cRegMessage.port))
	fmt.Printf("Connected to consumer at port: %v\n", cRegMessage.port)

	// Mỗi lần Consumer register là một consumer instance mới tham gia vào group.
	// Broker cần giữ connection này để gửi message trực tiếp về đúng Consumer đó.
	// status=false nghĩa là Consumer chưa được xem là ready để nhận message.
	// Khi Consumer ACK xong message trước đó, status sẽ được bật lại true.
	consumer := ConsumerConn{
		status: true,
		conn:   conn,
	}

	// Gắn connection này vào đúng group; một group có thể có nhiều ConsumerConn.
	// Consumption loop của group sẽ duyệt danh sách này để tìm Consumer đang ready.
	b.topics[topicIdx].cgroups[groupIdx].consumers = append(b.topics[topicIdx].cgroups[groupIdx].consumers, consumer)
	var resp byte = 0
	return &resp, nil
}

// startConsumerGroupConsumption chạy vòng lặp gửi message từ Topic tới Consumer trong một group.
// Input: topicIdx và cgroupIdx là index đã tìm/tạo ở bước Consumer register.
// Output: không return; goroutine này chạy liên tục cho tới khi process dừng.
// Cần hàm này để mỗi consumer group đọc topic queue theo offset riêng mà không pop queue chung.
// Hiện tại goroutine nằm ở cấp consumer group; từng ConsumerConn chỉ là connection được loop này dùng để gửi data.
func (b *Broker) startConsumerGroupConsumption(topicIdx uint, cgroupIdx uint) {
	fmt.Printf("Starting consumer group consumption for topic %d and group %d\n", topicIdx, cgroupIdx)
	for {
		b.topics[topicIdx].cgroups[cgroupIdx].lock.Lock()
		// Mỗi vòng loop thử lấy message tiếp theo cho group này.
		// offset thuộc về consumer group, không thuộc về topic queue.
		offset := b.topics[topicIdx].cgroups[cgroupIdx].offset

		// peek đọc message tại offset của group mà không pop khỏi topic queue.
		// Nhờ vậy group khác vẫn có thể đọc cùng message nếu offset của họ chưa vượt qua nó.
		pcm := b.topics[topicIdx].mq.peek(offset)
		if pcm == nil {
			time.Sleep(100 * time.Millisecond)
			b.topics[topicIdx].cgroups[cgroupIdx].lock.Unlock()
			continue
		}

		// Gửi message cho consumer đang ready, rồi chờ ACK trước khi bật ready lại.
		for _, consumer := range b.topics[topicIdx].cgroups[cgroupIdx].consumers {
			if consumer.status {
				// Tạo stream reader/writer trên connection riêng của Consumer hiện tại.
				streamRW := bufio.NewReadWriter(bufio.NewReader(consumer.conn), bufio.NewWriter(consumer.conn))
				// Đánh dấu Consumer bận trước khi gửi để tránh gửi thêm message khi chưa ACK.
				consumer.status = false
				// Gửi payload lấy từ topic queue sang Consumer bằng data message type hiện có.
				err := writeMessageToStream(streamRW, Message{PRODUCER_CONSUMER_MSG: pcm})
				if err != nil {
					fmt.Printf("Consumer disconnected while writing: %v\n", err)
					return
				}

				// Đọc ACK/status từ Consumer sau khi Consumer xử lý message.
				parsedMessage, err := readMessageFromStream(streamRW)

				// TODO: ACK handling đang là bản nháp; điều kiện này cần hoàn thiện cùng Consumer.
				if parsedMessage == nil || err != nil {
					fmt.Printf("Consumer disconnected while reading ack: %v\n", err)
					return
				}

				// Nếu nhận đúng ACK type, Consumer được xem là ready cho message tiếp theo.
				if parsedMessage.R_PRODUCER_CONSUMER_MSG != nil {
					consumer.status = true
				}
				// increased offset on consumer
				b.topics[topicIdx].cgroups[cgroupIdx].offset += 1
			} else {
				fmt.Printf("No consumer is ready, size = %d\n", len(b.topics[topicIdx].cgroups[cgroupIdx].consumers))
			}
		}
		b.topics[topicIdx].cgroups[cgroupIdx].lock.Unlock()
	}
}

// stopAndPop định kỳ dọn message cũ khỏi queue của một topic.
// Input: topicIdx là index của topic cần cleanup.
// Output: không return; goroutine này chạy nền và kiểm tra cleanup mỗi 5 giây.
// Cần hàm này để topic queue không giữ mãi các message mà mọi consumer group đã đọc xong.
func (b *Broker) stopAndPop(topicIdx uint) {
	for {
		time.Sleep(5 * time.Second)
		// minOffset là offset nhỏ nhất trong tất cả consumer group của topic.
		minOffset := -1
		for _, cgroup := range b.topics[topicIdx].cgroups {
			if minOffset == -1 {
				minOffset = int(cgroup.offset)
			} else if int(cgroup.offset) < minOffset {
				minOffset = int(cgroup.offset)
			}
		}
		fmt.Printf("Stop and pop run, minOffset = %d\n", minOffset)
		if minOffset != -1 {
			for i, _ := range b.topics[topicIdx].cgroups {
				b.topics[topicIdx].cgroups[i].lock.Lock()
				b.topics[topicIdx].cgroups[i].offset -= uint(minOffset)
			}
			// pop minOffset message khỏi đầu queue vì tất cả group đã consume qua các message đó.
			for minOffset > 0 {
				b.topics[topicIdx].mq.pop()
				minOffset -= 1
			}
			for i, _ := range b.topics[topicIdx].cgroups {
				b.topics[topicIdx].cgroups[i].lock.Unlock()
			}
		}
	}
}
