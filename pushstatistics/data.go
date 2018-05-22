package pushstatistics

import "sync"

// Categorys define 统计值
type Categorys struct {
	V   []int `json:"-"`
	Len uint  `json:"-"`

	Second int `json:"second"`
	Minute int `json:"minute"`
	Hour   int `json:"hour"`
	Day    int `json:"day"`
}

func newValue(length uint) *Categorys {
	return &Categorys{
		V:   make([]int, length),
		Len: length,
	}
}

// TickAt will 到下一秒
func (c *Categorys) TickAt(lastIndex, curIndex uint) {
	minute := (lastIndex + c.Len - 60) % c.Len
	hour := (lastIndex + c.Len - 3600) % c.Len
	day := (lastIndex + c.Len - 86400) % c.Len

	c.Second = 0
	c.Minute -= c.V[minute]
	c.Hour -= c.V[hour]
	c.Day -= c.V[day]

	c.V[curIndex] = 0
	return
}

// AddAt 在index位置添加count
func (c *Categorys) AddAt(index uint, count int) {
	c.V[index] += count

	c.Second += count
	c.Minute += count
	c.Hour += count
	c.Day += count
}

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
