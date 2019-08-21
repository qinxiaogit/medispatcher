package recoverwatcher

import (
	"medispatcher/broker"
	"medispatcher/broker/beanstalk"
	"medispatcher/config"
	"medispatcher/data"
	"medispatcher/logger"
	"reflect"
	"strings"
	"time"
)

// StartAndWait starts the recover process until Stop is called.
func StartAndWait() {
	exitWg.Add(1)
	var (
		redis          *data.RedisConn
		redisConnected bool
		err            error
		brPool         *beanstalk.SafeBrokerkPool
		msg            *data.MessageStuct
	)
	defer func() {
		err := recover()
		if err != nil {
			logger.GetLogger("ERROR").Printf("recoverwatcher exiting abnormally: %v", err)
		}
		exitWg.Done()
	}()
	brPool = broker.GetBrokerPoolWithBlock(2, INTERVAL_OF_RETRY_ON_CONN_FAIL, shouldExit)
	if brPool == nil {
		return
	}
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
			if strings.Index(err.Error(), "closed") != -1 || strings.Index(err.Error(), "EOF") != -1 {
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

			msg, err = data.UnserializeMessage(dataB)
			if err != nil {
				logger.GetLogger("WARN").Printf("Failed to decode msg from recover list: %v", err)
				continue
			}
			_, err = brPool.Pub(config.GetConfig().NameOfMainQueue, dataB, msg.Priority, msg.Delay, broker.DEFAULT_MSG_TTR)
			if err != nil {
				// job包的大小超过beanstalkd的限制.
				if strings.Index(strings.ToLower(err.Error()), "job_too_big") != -1 {
					logger.GetLogger("WARN").Printf("job data exceeds server-enforced limit: %v", dataB)
					continue
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
}
