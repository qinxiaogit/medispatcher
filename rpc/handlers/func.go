package handlers

import "sync"

var stoppingLocalServer bool
var stoppingLock = new(sync.Mutex)

func SetStoppingState() {
	defer stoppingLock.Unlock()
	stoppingLock.Lock()
	stoppingLocalServer = true
}

func isServerStopping() bool {
	stoppingLock.Lock()
	state := stoppingLocalServer
	stoppingLock.Unlock()
	return state
}
