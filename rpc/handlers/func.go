package handlers

var stoppingLocalServer bool
var stoppingLock chan uint8 = make(chan uint8, 1)

func SetStoppingState() {
	stoppingLock <- 1
	stoppingLocalServer = true
	<-stoppingLock
}

func isServerStopping() bool {
	stoppingLock <- 1
	state := stoppingLocalServer
	<-stoppingLock
	return state
}
