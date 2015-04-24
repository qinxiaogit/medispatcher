// Package recoverwatcher watches messages which previously failed sent to main message queue, and try to send them again.
package recoverwatcher

const(
	INTERVAL_OF_RETRY_ON_CONN_FAIL = 3
)

// If is in exiting status
var exiting bool

// exit signal chan, if receives 1, then the service should exit.
var exitChan = make(chan int8)

// lock for the exiting check.
var exitingCheckLock = make(chan int8, 1)