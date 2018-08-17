package pushstatistics

import "sync"

// Categorys define 统计值
type Categorys struct {
	V   []int `json:"-"`
	Len uint  `json:"-"`

	Second    int `json:"second"`
	TenSecond int `json:"10_second"`
	Minute    int `json:"minute"`
	Hour      int `json:"hour"`
	Day       int `json:"day"`
}

func newValue(length uint) *Categorys {
	return &Categorys{
		V:   make([]int, length),
		Len: length,
	}
}

// TickAt will 到下一秒
func (c *Categorys) TickAt(lastIndex, curIndex uint) {
	ten := (lastIndex + c.Len - 10) % c.Len
	minute := (lastIndex + c.Len - 60) % c.Len
	hour := (lastIndex + c.Len - 3600) % c.Len
	day := (lastIndex + c.Len - 86400) % c.Len

	c.Second = 0
	c.TenSecond -= c.V[ten]
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
	c.TenSecond += count
	c.Minute += count
	c.Hour += count
	c.Day += count
}

// CopyShow 只拷贝展示字段
func (c *Categorys) CopyShow() *Categorys {
	tmp := cPool.Get()
	tmp.Second = c.Second
	tmp.TenSecond = c.TenSecond
	tmp.Minute = c.Minute
	tmp.Hour = c.Hour
	tmp.Day = c.Day

	return tmp
}

// Recover 回收
func (c *Categorys) Recover() {
	cPool.Put(c)
}

// Reset 重置展示字段
func (c *Categorys) Reset() {
	c.Second = 0
	c.TenSecond = 0
	c.Minute = 0
	c.Hour = 0
	c.Day = 0
}

var (
	cPool = ShowCategorysPool{
		pool: sync.Pool{
			New: func() interface{} {
				return new(Categorys)
			},
		},
	}
)

// ShowCategorysPool pool
type ShowCategorysPool struct {
	pool sync.Pool
}

func (p *ShowCategorysPool) Get() *Categorys {
	out := p.pool.Get().(*Categorys)
	out.Reset()
	return out
}

func (p *ShowCategorysPool) Put(c *Categorys) {
	p.pool.Put(c)
}
