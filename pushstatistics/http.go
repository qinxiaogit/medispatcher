package pushstatistics

import (
	"fmt"
	"medispatcher/config"
	"medispatcher/logger"
	"net/http"
	"regexp"
)

func httpRun() {
	mux := http.NewServeMux()
	mux.HandleFunc("/prometheus/pushstatistics", GetPushStatistics)
	http.ListenAndServe(config.GetConfig().PrometheusApiAddr, mux)
}

// GetPushStatistics will 获取当前推送数据统计
// ?nozero=true 0值不传递
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

	out := makePrometheusFormat(ShowData(), req.URL.Query().Get("nozero") == "true")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(out))
	return
}

func makePrometheusFormat(values map[string]map[string]*Categorys, nozero bool) (out string) {
	/* go version >= 1.10
	var strBuilder strings.Builder
	for keyTopic, topics := range values {
		keyTopic = fmt.Sprintf("medispatcher_topic_%s", keyTopic)
		strBuilder.WriteString(fmt.Sprintf("# HELP %s A MQ topic.\n# TYPE %s topic\n", keyTopic, keyTopic))
		for keyChan, channels := range topics {
			strBuilder.WriteString(fmt.Sprintf("%s{channel=\"%s\",split=\"second\"} %d\n", keyTopic, keyChan, channels.Second))
			// strBuilder.WriteString(fmt.Sprintf("%s{channel=\"%s\",split=\"minute\"} %d\n", keyTopic, keyChan, channels.Minute))
		}
	}
	*/
	var strBuilder string
	for keyTopic, topics := range values {
		keyTopic = fmt.Sprintf("TOPIC:%s", fixTopicString(keyTopic))

		tmpStr := ""
		for keyChan, channels := range topics {
			if nozero && channels.Second == 0 {
				continue
			}
			tmpStr += fmt.Sprintf("%s{channel=\"%s\",split=\"second\"} %d\n", keyTopic, keyChan, channels.Second)
			// strBuilder.WriteString(fmt.Sprintf("%s{channel=\"%s\",split=\"minute\"} %d\n", keyTopic, keyChan, channels.Minute))
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
