package main

import (
	"bufio"
	"fmt"
)

const (
	ECHO                  = 1
	PRODUCER_REGISTER     = 2
	PRODUCER_CONSUMER_MSG = 3
	// response
	R_ECHO                  = 101
	R_PRODUCER_REGISTER     = 102
	R_PRODUCER_CONSUMER_MSG = 103
	// other message types
)

type Message struct {
	// Nếu ECHO != nil thì message này là echo request
	ECHO                    *string
	PRODUCER_REGISTER       *ProducerRegisterMessage
	PRODUCER_CONSUMER_MSG   []byte // nullable
	R_ECHO                  *string
	R_PRODUCER_REGISTER     *byte
	R_PRODUCER_CONSUMER_MSG *byte
}

type ProducerRegisterMessage struct {
	port    uint16
	topicID uint16
}

// fromByte decode payload đăng ký Producer từ bytes vào ProducerRegisterMessage.
// Input: stream_message phải có 4 bytes theo format [port cao][port thấp][topicID cao][topicID thấp].
// Output: không return; receiver m được cập nhật port và topicID.
// Ví dụ: []byte{0x27, 0x10, 0x00, 0x02} sẽ thành port=10000 và topicID=2.
func (m *ProducerRegisterMessage) fromByte(stream_message []byte) {
	// Payload có tổng cộng 4 bytes:
	// - 2 bytes đầu: producer port
	// - 2 bytes sau: topic ID
	// Mỗi uint16 dùng big-endian: byte cao đứng trước, byte thấp đứng sau.
	m.port = uint16(stream_message[0])<<8 + uint16(stream_message[1])
	m.topicID = uint16(stream_message[2])<<8 + uint16(stream_message[3])
}

// toByte encode ProducerRegisterMessage thành payload bytes để gửi qua TCP.
// Input: receiver m chứa port và topicID cần gửi.
// Output: trả về []byte dài 4 theo format [port cao][port thấp][topicID cao][topicID thấp].
// Ví dụ: port=10000 (0x2710), topicID=2 sẽ thành []byte{0x27, 0x10, 0x00, 0x02}.
func (m *ProducerRegisterMessage) toByte() []byte {
	// Encode ProducerRegisterMessage thành payload 4 bytes mà fromByte đọc được:
	// [port cao][port thấp][topicID cao][topicID thấp].
	var data = make([]byte, 4)
	data[0] = byte(m.port >> 8)
	data[1] = byte(m.port % 256)
	data[2] = byte(m.topicID >> 8)
	data[3] = byte(m.topicID % 256)
	return data
}

// readFromStream đọc một frame thô từ TCP stream theo format [length][data].
// Input: streamRW là buffered reader/writer đang bọc TCP connection.
// Output: trả về data bytes sau length header, hoặc error nếu stream lỗi/thiếu bytes.
// Cần hàm này để tách phần framing TCP khỏi phần parse message business-level.
func readFromStream(streamRW *bufio.ReadWriter) ([]byte, error) {
	var err error
	// Protocol: [length][data]
	header, err := streamRW.ReadByte() // block
	if err != nil {
		return nil, err
	}
	// Đọc đúng số byte mà header báo
	data, err := streamRW.Peek(int(header)) // block
	if err != nil {
		return nil, err
	}
	// Bỏ data đã đọc ra khỏi buffer
	_, err = streamRW.Discard(int(header))
	return data, err
}

// parseMessage chuyển raw bytes trong frame thành struct Message.
// Input: streamMessage có format [message_type][payload].
// Output: Message với field tương ứng được set, hoặc nil nếu type chưa hỗ trợ.
// Cần hàm này để broker/client làm việc với struct rõ nghĩa thay vì xử lý byte thủ công.
func parseMessage(streamMessage []byte) *Message {
	switch streamMessage[0] {
	case ECHO:
		var st = string(streamMessage[1:])
		return &Message{ECHO: &st}
	case R_ECHO:
		var st = string(streamMessage[1:])
		return &Message{R_ECHO: &st}
	case PRODUCER_REGISTER:
		p := ProducerRegisterMessage{}
		p.fromByte(streamMessage[1:])
		return &Message{PRODUCER_REGISTER: &p}
	case R_PRODUCER_REGISTER:
		var st = streamMessage[1]
		return &Message{R_PRODUCER_REGISTER: &st}
	case PRODUCER_CONSUMER_MSG:
		return &Message{PRODUCER_CONSUMER_MSG: streamMessage[1:]}
	case R_PRODUCER_CONSUMER_MSG:
		var st = streamMessage[1]
		return &Message{R_PRODUCER_CONSUMER_MSG: &st}
	default:
		return nil
	}
}

// readMessageFromStream đọc một frame từ stream rồi parse thành Message.
// Input: streamRW là buffered reader/writer đang bọc TCP connection.
// Output: Message đã parse hoặc error nếu đọc stream thất bại.
// Cần hàm này để gom hai bước read frame và parse message thành một API tiện dùng.
func readMessageFromStream(streamRW *bufio.ReadWriter) (*Message, error) {
	data, err := readFromStream(streamRW)
	if err != nil {
		return nil, err
	}
	return parseMessage(data), nil
}

// writeToStreamWithType encode và ghi một message cụ thể xuống TCP stream.
// Input: streamRW là TCP stream đã bọc buffer, message_type là loại message, data là payload.
// Output: trả error nếu ghi length/type/payload hoặc Flush thất bại.
// Cần hàm này để mọi message đều dùng chung format [length][message_type][payload].
func writeToStreamWithType(streamRW *bufio.ReadWriter, message_type byte, data string) error {
	var err error
	// Gửi frame theo protocol: [length][message_type][payload]
	err = streamRW.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}
	err = streamRW.WriteByte(message_type)
	if err != nil {
		return err
	}
	// Ghi nội dung payload
	_, err = streamRW.WriteString(data)
	if err != nil {
		return err
	}
	// Đẩy buffer ra TCP connection
	err = streamRW.Flush()
	if err != nil {
		return err
	}
	return nil
}

// writeMessageToStream chọn message type phù hợp rồi ghi Message xuống stream.
// Input: streamRW là TCP stream đã bọc buffer, message là struct chứa đúng một field cần gửi.
// Output: trả error nếu quá trình encode/write/flush thất bại.
// Cần hàm này để caller chỉ cần truyền Message, không phải tự biết byte type tương ứng.
func writeMessageToStream(streamRW *bufio.ReadWriter, message Message) error {
	if message.ECHO != nil {
		if err := writeToStreamWithType(streamRW, ECHO, *message.ECHO); err != nil {
			return err
		}
	}
	if message.R_ECHO != nil {
		if err := writeToStreamWithType(streamRW, R_ECHO, *message.R_ECHO); err != nil {
			return err
		}
	}
	if message.PRODUCER_REGISTER != nil {
		data := string(message.PRODUCER_REGISTER.toByte())
		if err := writeToStreamWithType(streamRW, PRODUCER_REGISTER, data); err != nil {
			return err
		}
	}
	if message.R_PRODUCER_REGISTER != nil {
		data := fmt.Sprintf("%d", *message.R_PRODUCER_REGISTER)
		if err := writeToStreamWithType(streamRW, R_PRODUCER_REGISTER, data); err != nil {
			return err
		}
	}
	if message.PRODUCER_CONSUMER_MSG != nil {
		if err := writeToStreamWithType(streamRW, PRODUCER_CONSUMER_MSG, string(message.PRODUCER_CONSUMER_MSG)); err != nil {
			return err
		}
	}
	if message.R_PRODUCER_CONSUMER_MSG != nil {
		data := fmt.Sprintf("%d", *message.R_PRODUCER_CONSUMER_MSG)
		if err := writeToStreamWithType(streamRW, R_PRODUCER_CONSUMER_MSG, data); err != nil {
			return err
		}
	}
	return nil
}
