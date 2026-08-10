package main

import "fmt"

const (
	maxMessageSize = 255
	queueCapacity  = 10000
)

var underArr = make([]byte, maxMessageSize*queueCapacity)
var underSize = make([]byte, maxMessageSize*queueCapacity)

type Queue struct {
	head uint32
	tail uint32
}

func (q *Queue) init() {
	q.head = 0
	q.tail = 0
}

// push ghi một message vào queue storage đã pre-allocate.
// Input: data là message bytes, giả định len(data) <= maxMessageSize.
// Output: không return; message được copy vào underArr tại vị trí q.tail.
// Cần hàm này để lưu message vào slot cố định 255 bytes và lưu độ dài thật trong underSize.
func (q *Queue) push(data []byte) {
	copy(underArr[q.tail:int(q.tail)+len(data)], data)
	underSize[q.tail] = byte(len(data))
	q.tail += maxMessageSize
	q.tail %= maxMessageSize * queueCapacity
}

// pop đọc message ở vị trí q.head rồi di chuyển head sang slot tiếp theo.
// Input: receiver Queue đang giữ head offset của message cần đọc.
// Output: trả về slice trỏ vào underArr với đúng length đã lưu trong underSize.
// Cần hàm này để lấy lại message thật thay vì đọc cả slot cố định 255 bytes.
func (q *Queue) pop() []byte {
	data := underArr[q.head : q.head+uint32(underSize[q.head])]
	q.head += maxMessageSize
	q.head %= maxMessageSize * queueCapacity
	return data
}

func (q *Queue) debug() {
	fmt.Printf("Debugging queue: \n")
	var cur = q.head
	for {
		data := underArr[cur : cur+uint32(underSize[cur])]
		fmt.Printf("%s\n", data)
		cur += maxMessageSize
		cur %= maxMessageSize * queueCapacity
		if cur >= q.tail {
			break
		}
	}
}
