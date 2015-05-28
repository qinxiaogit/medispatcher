package config

type Config struct {
	// ListenAddr is the network address for providing extra api calls.
	ListenAddr string
	InstallDir string
	PidFile    string
	LOG_DIR    string
	DATA_DIR   string

	QueueServerType      string
	QueueServerAddr      string
	NameOfMainQueue      string
	PrefixOfChannelQueue string

	// Database client settings
	//	Port     int32
	//	Host     string
	//	User     string
	//	Password string
	//	DbName 	  string
	Database map[string]interface{}

	// Redis client settings.
	//	Addr  string
	//  DbIndex uint8
	Redis map[string]interface{}

	DAEMON_USER string
	DAEMON_UID  int
	DAEMON_GID  int
	ClientVer   string

	ListenersOfMainQueue uint16

	MaxSendersPerChannel uint16

	SendersPerChannel uint16
	// In milliseconds
	IntervalOfSendingForSendRoutine uint16

	MaxSendersPerRetryChannel uint16

	SendersPerRetryChannel uint16

	// In milliseconds
	MaxMessageProcessTime uint16

	// interval = pow(retryTimes+1, 2)*CoeOfIntervalForRetrySendingMsg
	CoeOfIntervalForRetrySendingMsg uint16

	EnableMsgSentLog bool

	MaxRetryTimesOfSendingMessage uint16

	// list name for storing messages which failed sent to queue server.
	MsgQueueFaultToleranceListNamePrefix string
}

var config = &Config{
	QueueServerType:                      "beanstalk",
	ListenAddr:                           "0.0.0.0:5601",
	NameOfMainQueue:                      "main-incoming-queue",
	PrefixOfChannelQueue:                 "sub-queue/",
	QueueServerAddr:                      "127.0.0.1:11300",
	ListenersOfMainQueue:                 uint16(1),
	SendersPerChannel:                    uint16(1),
	MaxSendersPerChannel:                 uint16(10),
	IntervalOfSendingForSendRoutine:      uint16(1),
	SendersPerRetryChannel:               uint16(1),
	MaxSendersPerRetryChannel:            uint16(10),
	CoeOfIntervalForRetrySendingMsg:      uint16(10),
	EnableMsgSentLog:                     true,
	MaxRetryTimesOfSendingMessage:        uint16(10),
	MaxMessageProcessTime:                uint16(30000),
	MsgQueueFaultToleranceListNamePrefix: "mec_list_of_msg_for_restore_to_queue_server:",
	DATA_DIR: "/var/lib/medispatcher/",
}
