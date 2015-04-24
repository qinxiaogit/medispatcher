package recoverwatcher

import (
	"medispatcher/broker"
	"medispatcher/config"
	"medispatcher/data"
	"medispatcher/logger"
	"reflect"
	"strings"
	"time"
)

// StartAndWait starts the recover process until Stop is called.
func StartAndWait() {
	var (
		redisConnected bool
		redis          *data.RedisConn
		err            error
		brPt           *broker.Broker
		br             broker.Broker
		msg            data.MessageStuct
	)
	defer func(){
		err:= recover()
		if err != nil {
			logger.GetLogger("ERROR").Printf("recoverwatcher exiting abnormally: %v", err)
			exitChan <- 1
		}
	}()
	for !shouldExit() {
		err = nil
		if !redisConnected {
			redis, err = data.GetRedis()
		}

		if !redisConnected && err != nil {
			logger.GetLogger("WARN").Print(err)
			redisConnected = false
			time.Sleep(time.Second * INTERVAL_OF_RETRY_ON_CONN_FAIL)
			continue
		} else {
			redisConnected = true
		}
		keysRaw, err := redis.Do("KEYS", config.GetConfig().MsgQueueFaultToleranceListNamePrefix+"*")
		if err != nil {
			logger.GetLogger("WARN").Print(err)
			redisConnected = false
			continue
		}
		keys := keysRaw.([]interface{})
		var cmdArgKeys []interface{}
		if len(keys) > 0 {
			for _, k := range keys {
				cmdArgKeys = append(cmdArgKeys, k)
			}
		} else {
			time.Sleep(time.Second * 5)
			continue
		}

		cmdArgKeys = append(cmdArgKeys, "10")
		dataRaw, err := redis.Do("BRPOP", cmdArgKeys...)
		if err != nil {
			logger.GetLogger("WARN").Printf("%v: cmds: %v", err, cmdArgKeys)
			if strings.Index(err.Error(), "closed") != -1 || strings.Index(err.Error(), "EOF") != -1{
				redisConnected = false
			}
			time.Sleep(time.Second * 1)
			continue
		} else {
			if dataRaw == nil {
				continue
			}

			dataRawL, ok := dataRaw.([]interface{})
			if !ok {
				logger.GetLogger("WARN").Printf("Illformed data read from recover list. Expects []byte|[]uint8, but read " + reflect.TypeOf(dataRaw).String())
				logger.GetLogger("DATA").Printf("RECOVFAILDECODE %v", dataRawL[1])
				continue
			}
			var dataB []byte
			// TODO: []byte is to be expected, but msgpack sometimes returns string, so it needs assertion here.
			switch dataRawL[1].(type) {
			case string:
				dataB = []byte(dataRawL[1].(string))
			case []byte:
				dataB = dataRawL[1].([]byte)
			default:
				logger.GetLogger("DATA").Printf("RECOVFAILASSERT %v", dataRawL[1])
				continue
			}

			if brPt == nil {
				brPt = broker.GetBrokerWitBlock(INTERVAL_OF_RETRY_ON_CONN_FAIL, shouldExit)
				// maybe, in exiting stage
				if brPt == nil {
					// not able to put it to the queue server, then push it back to the recover list again.
					putBackSuccess := true
					_, err = redis.Do("RPUSH", dataRawL[0], dataB)
					if err != nil {
						// try again when redis connection problem.
						if strings.Index(err.Error(), "closed") != -1  || strings.Index(err.Error(), "EOF") != -1{
							redisConnected = false
							redis, err = data.GetRedis()
							if err == nil {
								_, err = redis.Do("RPUSH", dataRawL[0], dataB)
							}
							if err != nil {
								putBackSuccess = false
							}
						} else {
							putBackSuccess = false
						}
					}
					if !putBackSuccess{
						logger.GetLogger("WARN").Printf("Not able to push message back to recover list: %v", err)
						// message removed from queue, log it for later recovery in other means.
						logger.GetLogger("DATA").Printf("RECOVFAILBACK %v", dataB)
					}
					continue
				} else {
					br = *brPt
				}
				br.Use(config.GetConfig().NameOfMainQueue)
			}
			msg, err = data.UnserializeMessage(dataB)
			// TODO: msg loss risk
			if err != nil {
				logger.GetLogger("WARN").Printf("Failed to decode msg from recover list: %v", err)
				continue
			}
			_, err = br.Pub(msg.Priority, msg.Delay, broker.DEFAULT_MSG_TTR, dataB)
			if err != nil {
				if err.Error() == broker.ERROR_CONN_CLOSED {
					brPt = nil
				}
				// tailed to put to the queue server, then push it back to recover list again.
				_, err = redis.Do("RPUSH", dataRawL[0], dataB)
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to push msg back to recover list after try to push it to queue serever: %v", err)
					logger.GetLogger("DATA").Printf("RECOVFAILBACK %v", dataB)
				}
			}
		}
	}
	<-exitChan
}
