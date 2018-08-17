package pushstatistics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"

	l "github.com/sunreaver/gotools/logger"
)

func httpRun(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/prometheus/pushstatistics", GetPushStatistics)
	mux.HandleFunc("/prometheus/pushstatistics/withfail/10second", GetProbability10Second)
	mux.HandleFunc("/pushstatistics/alldata", GetPushStatisticsAllData)
	e := http.ListenAndServe(addr, mux)
	if e != nil {
		panic("pushstatistics http serve start error: " + e.Error())
	}
}

// GetPushStatistics will 获取当前推送数据统计
// ?nozero=true 0值不传递
func GetPushStatistics(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if e := recover(); e != nil {
			l.GetSugarLogger("push_statistics.log").Errorw("GetPushStatistics", "err", e)
		}
	}()

	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte(http.StatusText(http.StatusMethodNotAllowed)))
		return
	}

	show := ShowData(allSuffix)
	defer func() {
		// 回收复制出来的数据
		for k1 := range show {
			for k2 := range show[k1] {
				show[k1][k2].Recover()
			}
		}
	}()
	out := makePrometheusFormat(show,
		func(c *Categorys) (int, string) {
			return c.Second, "second"
		},
		req.URL.Query().Get("nozero") == "true")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(out))
	return
}

// GetProbability10Second 获取包含失败推送的概率统计数据
func GetProbability10Second(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if e := recover(); e != nil {
			l.GetSugarLogger("push_statistics.log").Errorw("GetFail10Second", "err", e)
		}
	}()

	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte(http.StatusText(http.StatusMethodNotAllowed)))
		return
	}

	show := ShowData(failSuffix)
	defer func() {
		// 回收复制出来的数据
		for k1 := range show {
			for k2 := range show[k1] {
				show[k1][k2].Recover()
			}
		}
	}()
	out := makePrometheusFormat(show,
		func(c *Categorys) (int, string) {
			return c.Second, "second"
		},
		req.URL.Query().Get("nozero") == "true")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(out))
}

// GetPushStatisticsAllData will 获取当前推送全量数据统计
func GetPushStatisticsAllData(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if e := recover(); e != nil {
			l.GetSugarLogger("push_statistics.log").Errorw("GetPushStatisticsAllData", "err", e)
		}
	}()

	var e error
	var out []byte
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		out = []byte(http.StatusText(http.StatusMethodNotAllowed))
	} else if out, e = json.Marshal(ShowData()); e != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out = []byte(e.Error())
	} else {
		w.WriteHeader(http.StatusOK)
	}

	w.Write([]byte(out))
	return
}

// suffix 筛选channel成功类型
// nozero 筛选值
func makePrometheusFormat(values map[string]map[string]*Categorys, fnValue func(*Categorys) (int, string), nozero bool) (out string) {
	var strBuilder string
	for keyTopic, topics := range values {
		keyTopic = fixTopicString(keyTopic)

		tmpStr := ""
		for keyChan, channels := range topics {
			value, split := fnValue(channels)
			if nozero && value == 0 {
				continue
			}
			tmpStr += fmt.Sprintf("%s{channel=\"%s\",split=\"%s\"} %d\n", keyTopic, keyChan, split, value)
		}
		if tmpStr != "" {
			strBuilder += fmt.Sprintf("# HELP %s A MQ topic.\n# TYPE %s topic\n", keyTopic, keyTopic) + tmpStr
		}
	}

	return strBuilder
}

func fixTopicString(topic string) string {
	re := regexp.MustCompile("([^a-zA-Z0-9_:]+)")
	return re.ReplaceAllString(topic, "_")
}
