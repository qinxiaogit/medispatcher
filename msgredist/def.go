package msgredist

import "sync"

const (
	// time interval (in seconds) for retry when failed connecting to queue server.
	INTERVAL_OF_RETRY_ON_CONN_FAIL = 3

	// timeout (in seconds) for reserving jog.
	DEFAULT_RESERVE_TIMEOUT = 1

	// maximum routines to redistribute messages from main incoming queue to subscription channel queues.
	MAX_REDIST_ROUTINES = 10

	// delay n seconds, when failing on re-distribute the message.
	DELAY_OF_RE_DISTRIBUTE_MESSAGE_ON_FAILURE = 1
)

var exitWg = new(sync.WaitGroup)

// exit signal chan
var exitChan = make(chan int8)