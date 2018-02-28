package config

import "medispatcher/Alerter"

type Config struct {
	// ListenAddr is the network address for providing extra api calls.
	ListenAddr       string
	DebugAddr        string
	StatisticApiAddr string
	InstallDir       string
	PidFile          string
	LOG_DIR          string
	DATA_DIR         string
	// Environment tag for the message reception(worker) addresses.
	RECEPTION_ENV string

	QueueServerType                string
	QueueServerAddr                string
	QueueServerPoolCmdConnCount    uint32
	QueueServerPoolListenConnCount uint32
	NameOfMainQueue                string
	PrefixOfChannelQueue           string

	// Database client settings
	//	Port     int32
	//	Host     string
	//	User     string
	//	Password string
	//	DbName 	  string
	//      MaxConn   int
	Database map[string]interface{}

	// Redis client settings.
	//	Addr  string
	//  DbIndex uint8
	Redis map[string]interface{}

	DAEMON_USER string
	DAEMON_UID  int
	DAEMON_GID  int
	ClientVer   string

	AlerterEmail Alerter.Config
	AlerterSms   Alerter.Config
	AlarmPlatform Alerter.Config

	ListenersOfMainQueue uint16

	MaxSendersPerChannel uint32

	SendersPerChannel uint32
	// In milliseconds
	IntervalOfSendingForSendRoutine uint32

	MaxSendersPerRetryChannel uint32

	SendersPerRetryChannel uint32

	// In milliseconds
	MaxMessageProcessTime uint32

	// In milliseconds
	DefaultMaxMessageProcessTime uint32

	// interval = pow(retryTimes+1, 2)*CoeOfIntervalForRetrySendingMsg
	CoeOfIntervalForRetrySendingMsg uint16

	EnableMsgSentLog bool

	MaxRetryTimesOfSendingMessage uint16

	// list name for storing messages which failed sent to queue server.
	MsgQueueFaultToleranceListNamePrefix string

	// 是否按天切分日志
	SplitLog bool
}
