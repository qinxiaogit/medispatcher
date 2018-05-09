package pushstatistics

import (
	"fmt"
	"medispatcher/config"
	"medispatcher/logger"
	"net/http"
	"strings"
)

func httpRun() {
	http.HandleFunc("/prometheus/pushstatistics", GetPushStatistics)
	http.ListenAndServe(config.GetConfig().PrometheusApiAddr, nil)
}

// GetPushStatistics will 获取当前推送数据统计
func GetPushStatistics(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if e := recover(); e != nil {
			logger.GetLogger("ERROR").Printf("GetPushStatistics: %v", e)
		}
	}()

	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte(http.StatusText(http.StatusMethodNotAllowed)))
		return
	}
	out := makePrometheusFormat(ShowData())
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(out))
	return
}

func makePrometheusFormat(values map[string]map[string]*Categorys) (out string) {
	var strBuilder strings.Builder
	for keyTopic, topics := range values {
		keyTopic = fmt.Sprintf("medispatcher_topic_%s", keyTopic)
		strBuilder.WriteString(fmt.Sprintf("# HELP %s A MQ topic.\n# TYPE %s topic\n", keyTopic, keyTopic))
		for keyChan, channels := range topics {
			strBuilder.WriteString(fmt.Sprintf("%s{channel=\"%s\",split=\"second\"} %d\n", keyTopic, keyChan, channels.Second))
			// strBuilder.WriteString(fmt.Sprintf("%s{channel=\"%s\",split=\"minute\"} %d\n", keyTopic, keyChan, channels.Minute))
		}
	}

	return strBuilder.String()
}
