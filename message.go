package main

import "bufio"

const (
	ECHO              = 1
	PRODUCER_REGISTER = 2
	// response
	R_ECHO              = 101
	R_PRODUCER_REGISTER = 102
	// other message types
)

type Message struct {
	// Nếu ECHO != nil thì message này là echo request
	ECHO                *string
	PRODUCER_REGISTER   *string
	R_ECHO              *string
	R_PRODUCER_REGISTER *byte
}

// readFromStream đọc một frame thô từ TCP stream theo format [length][data].
// Input: stream_rw là buffered reader/writer đang bọc TCP connection.
// Output: trả về data bytes sau length header, hoặc error nếu stream lỗi/thiếu bytes.
// Cần hàm này để tách phần framing TCP khỏi phần parse message business-level.
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

// parseMessage chuyển raw bytes trong frame thành struct Message.
// Input: stream_message có format [message_type][payload].
// Output: Message với field tương ứng được set, hoặc nil nếu type chưa hỗ trợ.
// Cần hàm này để broker/client làm việc với struct rõ nghĩa thay vì xử lý byte thủ công.
func parseMessage(stream_message []byte) *Message {
	switch stream_message[0] {
	case ECHO:
		var st = string(stream_message[1:])
		return &Message{ECHO: &st}
	case R_ECHO:
		var st = string(stream_message[1:])
		return &Message{R_ECHO: &st}
	case PRODUCER_REGISTER:
		var st = string(stream_message[1:])
		return &Message{PRODUCER_REGISTER: &st}
	case R_PRODUCER_REGISTER:
		var st = stream_message[1]
		return &Message{R_PRODUCER_REGISTER: &st}
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
		if err := writeToStreamWithType(streamRW, PRODUCER_REGISTER, *message.PRODUCER_REGISTER); err != nil {
			return err
		}
	}
	if message.R_PRODUCER_REGISTER != nil {
		data := string(*message.R_PRODUCER_REGISTER)
		if err := writeToStreamWithType(streamRW, R_PRODUCER_REGISTER, data); err != nil {
			return err
		}
	}
	return nil
}
