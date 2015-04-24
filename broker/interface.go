// Package broker provide unified interface for specified message queue client.
//	currently supported message queue servers are beanstalkd.
package broker

// Broker interface is a layer that is compatible with beanstalkd features.
type Broker interface {
	Watch(topicName string) error
	UnWatch(topicName string) error
	// Release put the reserved job back to its original ready queue.
	Release(jobId uint64, priority uint32, delay uint64) error
	// Use change the current using topic to which is for topicName, and the subsequential messages will be published to this topic queue.
	Use(topicName string) error
	Pub(pri uint32, delay, ttr uint64, body []byte) (jobId uint64, err error)
	// Reserve occupies a job exclusively.
	Reserve() (jobId uint64, jobBody []byte, err error)
	ReserveWithTimeout(seconds int) (jobId uint64, jobBody []byte, err error)
	ListTopics() (topics []string, err error)
	// Stats show the queue server info.
	Stats() (stats map[string]interface{}, err error)
	StatsJob(jobId uint64) (stats map[string]interface{}, err error)
	StatsTopic(topicName string) (stats map[string]interface{}, err error)
	Delete(jobId uint64) (err error)
	Bury(jobId uint64) (err error)
	Close()
}
