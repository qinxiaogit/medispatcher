package msgredist

import ()

// Stop stops the redispatch.
func Stop(returnCh *chan string) {
	// wait for the exit signal to be checked. taken by shouldExit
	exitChan <- int8(1)
	// wait for the process exit.   token by StartAndWait
	exitChan <- int8(1)
	*returnCh <- "msgredist"
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
