package config

import "medispatcher/Alerter"

type Config struct {
	// ListenAddr is the network address for providing extra api calls.
	ListenAddr string
	StatisticApiAddr string
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

	AlerterEmail Alerter.Config
	AlerterSms   Alerter.Config

	ListenersOfMainQueue uint16

	MaxSendersPerChannel uint16

	SendersPerChannel uint16
	// In milliseconds
	IntervalOfSendingForSendRoutine uint16

	MaxSendersPerRetryChannel uint16

	SendersPerRetryChannel uint16

	// In milliseconds
	MaxMessageProcessTime uint16

	// In milliseconds
	DefaultMaxMessageProcessTime uint16

	// interval = pow(retryTimes+1, 2)*CoeOfIntervalForRetrySendingMsg
	CoeOfIntervalForRetrySendingMsg uint16

	EnableMsgSentLog bool

	MaxRetryTimesOfSendingMessage uint16

	// list name for storing messages which failed sent to queue server.
	MsgQueueFaultToleranceListNamePrefix string
}
