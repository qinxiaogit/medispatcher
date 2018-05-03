package pushstatistics

import (
	"container/list"
	"sync"
)

// MessageQueue define 消息队列的值
type MessageQueue interface {
	GetTopic() string
	GetChannel() string
}

// MQList define 待计算的值
type MQList struct {
	sync.RWMutex
	Data *list.List
}

// NewMQList will NewMQList
func NewMQList() *MQList {
	return &MQList{
		Data: list.New(),
	}
}

func (m *MQList) push(mq MessageQueue) {
	m.Lock()
	m.Unlock()
	m.Data.PushBack(mq)
}

// GetAllData will 获取所有推送数据
func (m *MQList) GetAllData() *list.List {
	m.Lock()
	defer m.Unlock()
	tmp := m.Data
	m.Data = list.New()
	return tmp
}
