package sender

import "sync"

var procExitWG = new(sync.WaitGroup)

// exit signal chan, if receives 1, then the service should exit.
var exitChan = make(chan int8)

const (
	SENDER_ROUTINE_SIG_INCREASE_ROUTINE           = SubSenderRoutineChanSig(1)
	SENDER_ROUTINE_SIG_DECREASE_ROUTINE           = SubSenderRoutineChanSig(2)
	SENDER_ROUTINE_SIG_EXIT_ALL_ROUTINES          = SubSenderRoutineChanSig(3)
	SENDER_ROUTINE_SIG_EXIT                       = SubSenderRoutineChanSig(10)
	SENDER_ROUTINE_SIG_EXITED                     = SubSenderRoutineChanSig(11)
	SENDER_ROUTINE_SIG_INCREASE_ROUTINE_FOR_RETRY = SubSenderRoutineChanSig(4)
	SENDER_ROUTINE_SIG_DECREASE_ROUTINE_FOR_RETRY = SubSenderRoutineChanSig(5)
)

const (
	INTERVAL_OF_RETRY_ON_CONN_FAIL = 3
	DEFAULT_RESERVE_TIMEOUT        = 1
)

const (
	// 报警间隔
	ALARM_INTERVAL = 3 * 60
	// Alert interval(in seconds) for an alert.
	INTERVAL_OF_ERROR_MONITOR_ALERT = 3 * 60
	// When the message sent failed times reaches the threshold the alert should be sent.
	MESSAGE_FAILURE_ALERT_THRESHOLD = 7
	// When sending messages for the subscription failed and the failed times reached the threshold in a specified period,
	// the alert should be sent.
	SUBSCRIPTION_TOTAL_FAILURE_ALERT_THRESHOLD = 120
	MESSAGE_BLOCKED_ALERT_THRESHOLD            = 5000
)

var senderRoutineStats = &SenderRoutineStats{
	routineStatus: map[int32]*StatusOfSubSenderRoutine{},
	lockChan: func() *chan bool {
		ch := make(chan bool, 1)
		return &ch
	}(),
}

var senderErrorMonitor *errorMonitor
var topicStats TopicStats
