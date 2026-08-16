package main

import "net"

type CGroup struct {
	groupID   uint16
	offset    uint
	consumers []ConsumerConn
}

type ConsumerConn struct {
	status bool
	conn   net.Conn
}

func (c *CGroup) init(group_id uint16) {
	c.groupID = group_id
	c.offset = 0
}
