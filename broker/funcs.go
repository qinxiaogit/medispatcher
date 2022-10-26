package broker

import (
	"errors"
	"github.com/qinxiaogit/medispatcher/broker/beanstalk"
	"github.com/qinxiaogit/medispatcher/config"
	"github.com/qinxiaogit/medispatcher/logger"
	"time"
)

func GetValidBrokers() map[string]BrokerInfo {
	return validBrokers
}

// New create a new broker
// addr network address, e.g. 127.0.0.1:11300
func New(config Config) (br Broker, err error) {
	if _, exists := validBrokers[config.Type]; !exists {
		err = errors.New(Error_InvalidBroker)
		return nil, err
	}
	switch config.Type {
	case BrokerName_Beanstalkd:
		br, err = beanstalk.New(config.Addr)
	}
	return
}

func NewBrokerPool(config Config, concurrency uint32) (br *beanstalk.SafeBrokerkPool, err error) {
	return beanstalk.NewSafeBrokerPool(config.Addr, concurrency)
}

// Normalize job stats type. It's affected by yaml parser.
func NormalizeJobStats(stats *map[string]interface{}) {
	switch (*stats)["ttr"].(type) {
	case int:
		(*stats)["ttr"] = uint64((*stats)["ttr"].(int))
	case uint64:
		(*stats)["ttr"] = (*stats)["ttr"].(uint64)
	}

	switch (*stats)["pri"].(type) {
	case int:
		(*stats)["pri"] = uint32((*stats)["pri"].(int))
	case uint64:
		(*stats)["pri"] = uint32((*stats)["pri"].(uint64))
	}

	switch (*stats)["delay"].(type) {
	case int:
		(*stats)["delay"] = uint64((*stats)["delay"].(int))
	case uint64:
		(*stats)["delay"] = (*stats)["delay"].(uint64)
	}
}

//
// concurrency concurrent connections on each endpoint.
func GetBrokerPoolWithBlock(concurrency uint32, retryInteral int, exitCheck func() bool) (brPool *beanstalk.SafeBrokerkPool) {
	if exitCheck() {
		return
	}
	var err error
	for !exitCheck() {
		st := time.Now()
		brPool, err = beanstalk.NewSafeBrokerPool(config.GetConfig().QueueServerAddr, concurrency)
		if err == nil {
			return
		} else {
			logger.GetLogger("WARN").Printf("Failed to create broker pool: %v", err)
			elapsed := time.Now().Sub(st)
			du := time.Second * time.Duration(retryInteral)
			if elapsed < du {
				<-time.After(du - elapsed)
			}
		}
	}
	return
}

// GetBroker get a new broker. It will block until a broker is successfully created or Stop is called.
// retryInterval is in seconds.
// exitCheck returns true will break the block.
// safeBroker whether return a coroutine safe broker.
// returns error when exiting
func GetBrokerWithBlock(retryInteral int, exitCheck func() bool) (br Broker, err error) {
	if exitCheck() {
		err = errors.New("Exiting!")
		return
	}
	brokerConfig := Config{
		Type: config.GetConfig().QueueServerType,
		Addr: config.GetConfig().QueueServerAddr,
	}
	hasConnTestLock := getBorkerConnAvailTestLock()
	if hasConnTestLock {
		defer func() {
			setBrokerConnAvailable()
			releaseBorkerConnAvailTestLock()
		}()
	}

	for !exitCheck() {
		if !hasConnTestLock {
			blockMoreBrokerConnFailures()
			// block broken and check the exit signal again.
			time.Sleep(time.Millisecond * 100)
			if exitCheck() {
				err = errors.New("Exiting!")
				return
			}
		}
		br, err = New(brokerConfig)
		if err == nil {
			break
		} else {
			if hasConnTestLock {
				setBrokerUnavailable()
			}

			logger.GetLogger("WARN").Printf("Failed to create broker: %v. Retry in %v seconds...", err, retryInteral)
			time.Sleep(time.Second * time.Duration(retryInteral))
		}
	}
	return
}

func getBorkerConnAvailTestLock() bool {
	var locked bool
	select {
	case borkerConnAvailTestLock <- 1:
		locked = true
	default:
		locked = false
	}
	return locked
}

func releaseBorkerConnAvailTestLock() {
	<-borkerConnAvailTestLock
}

func blockMoreBrokerConnFailures() {
	for !isBrokerConnAvailable() {
		time.Sleep(time.Millisecond * 40)
	}
}

func setBrokerConnAvailable() {
	brokerConnAvailTestRWLock <- 1
	brokerConnAvail = true
	<-brokerConnAvailTestRWLock
}

func setBrokerUnavailable() {
	brokerConnAvailTestRWLock <- 1
	brokerConnAvail = false
	<-brokerConnAvailTestRWLock
}

func isBrokerConnAvailable() bool {
	brokerConnAvailTestRWLock <- 1
	avail := brokerConnAvail
	<-brokerConnAvailTestRWLock
	return avail
}
