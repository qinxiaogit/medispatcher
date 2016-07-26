// Package recoverwatcher watches messages which previously failed sent to main message queue, and try to send them again.
package recoverwatcher

import "sync"

const(
	INTERVAL_OF_RETRY_ON_CONN_FAIL = 3
)

// exit signal chan, if receives 1, then the service should exit.
var exitChan = make(chan int8)

var exitWg = new(sync.WaitGroup)