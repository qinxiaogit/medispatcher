package recoverwatcher

import ()

// Stop stops the recover watcher.
func Stop(returnCh *chan string) {
	close(exitChan)
	exitWg.Wait()
	*returnCh <- "recoverwatcher"
}

func shouldExit() bool {
	select {
	case <-exitChan:
		return true
	default:
		return false
	}
}
