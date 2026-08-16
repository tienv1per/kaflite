package main

import (
	"net"
	"sync"
)

type CGroup struct {
	groupID   uint16
	offset    uint
	consumers []ConsumerConn
	lock      sync.Mutex
}

type ConsumerConn struct {
	status bool
	conn   net.Conn
}

func (c *CGroup) init(group_id uint16) {
	c.groupID = group_id
	c.offset = 0
}
