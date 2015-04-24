package recoverwatcher

import ()

// Stop stops the recover watcher.
func Stop(returnCh *chan string) {
	// test shouldExit first, in case exitChan used in panic/recover stage.
	if !shouldExit(){
		// wait for the exit signal to be checked. taken by shouldExit
		exitChan <- int8(1)
		// wait for the process exit.   token by StartAndWait
		exitChan <- int8(1)
	}
	*returnCh <- "recoverwatcher"
}

func shouldExit() bool {
	exitingCheckLock <- 1
	r := exiting
	if !r {
		select {
		case <-exitChan:
			exiting = true
			break
		default:
		}
		r = exiting
	}
	<-exitingCheckLock
	return r
}
