package pushstatistics

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type test1 struct {
	t, c string
}

func newTest1() *test1 {
	rand.Seed(time.Now().UnixNano())
	return &test1{
		t: string(rand.Uint32()%26 + 'a'),
		c: string(rand.Uint32()%26 + 'a'),
	}
}

func (t *test1) GetTopic() string {
	return t.t
}

func (t *test1) GetChannel() string {
	return t.c
}

func TestMain(t *testing.T) {
	PrometheusStatisticsStart(":0")
}

func TestAdd(t *testing.T) {
	Convey("Add Test", t, func() {
		Convey("Mutex Add", func() {
			testCount := 800

			var wg sync.WaitGroup

			rand.Seed(time.Now().UnixNano())
			cur := map[string]map[string]int{}
			var curLock sync.Mutex
			for i := 0; i < testCount/2; i++ {
				wg.Add(1)
				go func() {
					time.Sleep(time.Second * time.Duration(rand.Uint32()%120))
					test := newTest1()
					Add(test, 1, true)
					curLock.Lock()
					if _, ok := cur[test.GetTopic()]; !ok {
						cur[test.GetTopic()] = map[string]int{}
					}
					cur[test.GetTopic()][test.GetChannel()+allSuffix]++
					cur[test.GetTopic()][test.GetChannel()+successSuffix]++
					curLock.Unlock()

					wg.Done()
				}()
			}
			for i := 0; i < testCount/2; i++ {
				wg.Add(1)
				go func() {
					time.Sleep(time.Second * time.Duration(rand.Uint32()%120))
					test := newTest1()
					Add(test, 1, false)
					curLock.Lock()
					if _, ok := cur[test.GetTopic()]; !ok {
						cur[test.GetTopic()] = map[string]int{}
					}
					cur[test.GetTopic()][test.GetChannel()+failSuffix]++
					cur[test.GetTopic()][test.GetChannel()+allSuffix]++
					curLock.Unlock()
					wg.Done()
				}()
			}
			wg.Wait()

			time.Sleep(time.Second * 3)
			data := ShowData(allSuffix, successSuffix, failSuffix)

			for t, channels := range cur {
				for c, count := range channels {
					_, ok := data[t]
					So(ok, ShouldBeTrue)

					item, ok := data[t][c]
					So(ok, ShouldBeTrue)

					So(item.Day, ShouldEqual, count)
					So(item.Hour, ShouldEqual, count)
					So(item.Minute, ShouldBeLessThanOrEqualTo, count)
					// 因为上面有3秒sleep
					So(item.Second, ShouldBeLessThan, count)
				}
			}

			sumD := 0
			sumH := 0
			sumM := 0
			for _, channels := range data {
				for c, count := range channels {
					if strings.HasSuffix(c, allSuffix) {
						sumD += count.Day
						sumH += count.Hour
						sumM += count.Minute
					}
				}
			}
			So(sumD, ShouldEqual, testCount)
			So(sumH, ShouldEqual, testCount)
			So(sumM, ShouldBeLessThanOrEqualTo, testCount)

			out, _ := json.MarshalIndent(data, "", "  ")
			fmt.Println("\n\n\n", string(out))
		})
	})
}
