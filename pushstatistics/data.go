package pushstatistics

import "sync"

// Statistics define 统计数据
type Statistics struct {
	sync.RWMutex
	Index uint
	Len   uint
	// Data
	// [topic][channel]Categorys
	Data map[string]map[string]*Categorys
}

func (s *Statistics) channel(topic, c string) *Categorys {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.Data[topic]; !ok {
		s.Data[topic] = map[string]*Categorys{}
	}

	if _, ok := s.Data[topic][c]; !ok {
		v := newValue(s.Len)
		s.Data[topic][c] = v
	}
	return s.Data[topic][c]
}

// GC 清除0值的map
func (s *Statistics) GC() {
	s.Lock()
	defer s.Unlock()

	for topic, channels := range s.Data {
		topicCount := len(channels)
		for channel, data := range channels {
			if data.Day == 0 {
				topicCount--
				delete(channels, channel)
			}
		}
		if topicCount == 0 {
			delete(s.Data, topic)
		}
	}
}

// Tick will 移动所有的topic下的所有channel的游标
func (s *Statistics) Tick() (lastIndex uint) {
	s.Lock()
	defer s.Unlock()

	lastIndex = s.Index
	s.Index++
	s.Index %= s.Len

	for _, topics := range s.Data {
		for _, channels := range topics {
			channels.TickAt(lastIndex, s.Index)
		}
	}

	return
}

// Show will 返回统计数据
func (s *Statistics) Show() map[string]map[string]*Categorys {
	s.RLock()
	defer s.RUnlock()
	return s.Data
}

// ShowCopy will 返回统计数据(copy方式)
func (s *Statistics) ShowCopy() map[string]map[string]*Categorys {
	data := s.Show()
	tmp := map[string]map[string]*Categorys{}

	for k1, v1 := range data {
		for k2 := range v1 {
			if _, ok := tmp[k1]; !ok {
				tmp[k1] = map[string]*Categorys{}
			}
			tmp[k1][k2] = data[k1][k2].CopyShow()
		}
	}
	return tmp
}
