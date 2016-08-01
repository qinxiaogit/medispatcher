package broker

var validBrokers = map[string]BrokerInfo{"beanstalk": BrokerInfo{"beanstalk"}}

var brokerConnAvailTestRWLock = make(chan int8, 1)

// it is who only has the lock can the broker connection availability.
var borkerConnAvailTestLock = make(chan int8, 1)

// If the connection to broker is available.
var brokerConnAvail = false

const (
	Error_InvalidBroker = "Invalid broker."

	ERROR_CONN_CLOSED         = "EOF"
	ERROR_CONN_BROKEN         = "broken pipe"
	ERROR_JOB_RESERVE_TIMEOUT = "TIMED_OUT"
	ERROR_JOB_NOT_FOUND       = "NOT_FOUND"
	ERROR_QUEUE_NOT_FOUND     = "NOT_FOUND"
)

const (
	DEFAULT_MSG_PRIORITY = uint32(1024)
	DEFAULT_MSG_TTR      = uint64(60)
)
const (
	BrokerName_Beanstalkd = "beanstalk"
)
