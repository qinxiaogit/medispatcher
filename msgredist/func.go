package msgredist

import ()

// Stop stops the redispatch.
func Stop(returnCh *chan string) {
	close(exitChan)
	exitWg.Wait()
	*returnCh <- "msgredist"
}

func shouldExit() bool {
	select {
	case <-exitChan:
		return true
	default:
		return false
	}
}
