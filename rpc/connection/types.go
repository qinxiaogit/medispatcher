package connection

import(
	"sync"
)
type ConnActionStatus int8

type ProcessResult struct {
	Result []byte
	Error  error
}

type Request struct {
	Closed bool
	Body   []byte
	Err    error
}

type StoppingSigalOp struct {
	lockChan      *chan bool
	userCounter   uint64
	stoppingSigal chan bool
}

type StatsMessage struct {
	cmd    string
	action ConnAction
	status ConnActionStatus
}

type Stats struct {
	TotalReq    int
	FinishedReq int
	FailedReq   int
}

type StatsSegs struct  {
	segs  map[int64]map[string]Stats
	lock *sync.Mutex
}

func (ss StatsSegs)AddStats(index int64, statsMsg *StatsMessage){
	ss.lock.Lock()
	defer ss.lock.Unlock()
	statsSeg, exists := ss.segs[index]
	if !exists{
		statsSeg = map[string]Stats{}
	}
	stats, exists := statsSeg[(*statsMsg).cmd]
	if !exists {
		stats = Stats{0, 0, 0}
	}

	stats.TotalReq += 1
	if (*statsMsg).status == CONNACTION_STATUS_OK {
		stats.FinishedReq += 1
	} else {
		stats.FailedReq += 1
	}
	statsSeg[(*statsMsg).cmd] = stats
	ss.segs[index] = statsSeg
}

func (ss StatsSegs)GetSegs()map[int64]map[string]Stats{
	ss.lock.Lock()
	defer ss.lock.Unlock()
	return ss.segs
}

func (ss StatsSegs)SegLen()int{
	ss.lock.Lock()
	defer ss.lock.Unlock()
	return len(ss.segs)
}

func (ss StatsSegs)DeleteSeg(index int64){
	ss.lock.Lock()
	defer ss.lock.Unlock()
	delete(ss.segs, index)
}