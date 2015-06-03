package sender

// exit signal chan, if receives 1, then the service should exit.
var exitChan = make(chan int8)

// If is in exiting status
var exiting bool

// lock for the exiting check.
var exitingCheckLock = make(chan int8, 1)

const (
	SENDER_ROUTINE_SIG_INCREASE_ROUTINE           = SubSenderRoutineChanSig(1)
	SENDER_ROUTINE_SIG_DECREASE_ROUTINE           = SubSenderRoutineChanSig(2)
	SENDER_ROUTINE_SIG_EXIT_ALL_ROUTINES          = SubSenderRoutineChanSig(3)
	SENDER_ROUTINE_SIG_EXIT                       = SubSenderRoutineChanSig(10)
	SENDER_ROUTINE_SIG_EXITED_ABNORMALLY          = SubSenderRoutineChanSig(12)
	SENDER_ROUTINE_SIG_EXITED                     = SubSenderRoutineChanSig(11)
	SENDER_ROUTINE_SIG_INCREASE_ROUTINE_FOR_RETRY = SubSenderRoutineChanSig(4)
	SENDER_ROUTINE_SIG_DECREASE_ROUTINE_FOR_RETRY = SubSenderRoutineChanSig(5)
)

const (
	INTERVAL_OF_RETRY_ON_CONN_FAIL = 3
	DEFAULT_RESERVE_TIMEOUT        = 1
)

var senderRoutineStats = &SenderRoutineStats{
	routineStatus: map[int32]*StatusOfSubSenderRoutine{},
	lockChan: func()*chan bool {
		ch := make(chan bool, 1)
		return &ch
	}(),
}
