package pushstatistics

import "sync"

// Categorys define 统计值
type Categorys struct {
	V     []int `json:"-"`
	Index uint  `json:"-"`
	Len   uint  `json:"-"`

	Second int `json:"second"`
	Minute int `json:"minute"`
	Hour   int `json:"hour"`
	Day    int `json:"day"`
}

func newValue(length uint) *Categorys {
	return &Categorys{
		V:     make([]int, length),
		Len:   length,
		Index: 0,
	}
}

// Tick will 到下一秒
func (c *Categorys) Tick() {
	c.Index++
	c.Index %= c.Len

	minute := (c.Index + c.Len - 60) % c.Len
	hour := (c.Index + c.Len - 3600) % c.Len
	day := (c.Index + c.Len - 86400) % c.Len

	c.V[c.Index] = 0
	c.Second = 0
	c.Minute -= c.V[minute]
	c.Hour -= c.V[hour]
	c.Day -= c.V[day]
}

// Add 添加
func (c *Categorys) Add(count int) {
	c.V[c.Index]++
	c.Second++
	c.Minute++
	c.Hour++
	c.Day++
}

// Statistics define 统计数据
type Statistics struct {
	sync.RWMutex
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
		v := newValue(24 * 3600)
		s.Data[topic][c] = v
	}
	return s.Data[topic][c]
}

// Tick will 移动所有的topic下的所有channel的游标
func (s *Statistics) Tick() {
	s.Lock()
	defer s.Unlock()
	for _, topic := range s.Data {
		for _, channel := range topic {
			channel.Tick()
		}
	}
}

// Show will 返回统计数据
func (s *Statistics) Show() map[string]map[string]*Categorys {
	s.RLock()
	defer s.RUnlock()
	return s.Data
}
