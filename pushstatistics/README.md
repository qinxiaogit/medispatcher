# 消息中心（medispatcher）统计

---

## 1.需统计内容

topic | channel | 实时推送量(上1秒) | 1分钟 | 1小时 | 1天
:---: | :---: | :---: | :---: | :---: | :---:
xxx | xxx | 1 | 60 | 3600 | 86400

## 2.获取分时数据

Method: `rpc`
Function: `GetPushStatistics`

## 3.获取prometheus格式数据

Method: `HTTP`
Function: `curl -X GET http://host:25606/prometheus/pushstatistics`
Response header: `Content-Type: text/plain; charset=utf-8`
Response data Example:

```
# HELP medispatcher_topic_X A MQ topic.
# TYPE medispatcher_topic_X topic
medispatcher_topic_X{channel="somekey1",split="second"} 4
medispatcher_topic_X{channel="somekey2",split="second"} 5
```
