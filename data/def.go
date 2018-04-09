// Package data provides any data operations and definitions that need by the dispatcher service. Such as information of subscriber, subscriptions , data service access layer are within, message structures etc.
package data

const (
	MSG_SERIALIZER = "msgpack"
)

const (
	// maximum database connections
	MAX_DB_CONNECTIONS                         = 10
)

const (
	CACHE_KEY_PREFIX_SUBSCRIPTIONS = "subscriptions_"
	CACHE_KEY_PREFIX_SUBSCRIBER    = "subscriber_"
)

const (
	DB_TABLE_SUBSCRIPTIONS         = "subscriptions"
	DB_TABLE_MESSAGE_CLASSES       = "message_classes"
	DB_TABLE_BROADCAST_FAILURE_LOG = "broadcast_failure_log"
	DB_TABLE_SUBSCRIBERS           = "subscribers"
	DB_TABLE_SUBSCRIPTION_PARAMS   = "subscription_params"
)

const (
	SUBSCRIPTION_NORMAL = 0
	SUBSCRIPTION_CANCEL = 1
)

const(
	REDIS_WRITE_COMMANDS = ",SET,LSET,LPUSH,RPUSH,"
	REDIS_READ_COMMANDS = ",GET,LPOP,RPOP,BLPOP,BRPOP,"
)

type MessageStuct struct {
	MsgKey string
	Body   interface{}
	Sender string
	// Time is the (in unix timestamp format) time when the message is received from the publisher.
	Time float64
	// jobid from the main queue.
	OriginJobId uint64
	RetryTimes  uint16
	// log id of failure logs.
	LogId    uint64
	Delay    uint64
	Priority uint32

	Context string
	Owl_context string
}
