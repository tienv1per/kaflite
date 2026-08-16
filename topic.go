package main

import "sync"

type Topic struct {
	topicID uint16
	mq      Queue
	cgroups []*CGroup
	lock    sync.Mutex
}

func (t *Topic) init(topic_id uint16) {
	t.topicID = topic_id
	t.mq.init()
	t.cgroups = make([]*CGroup, 0)
}
