// Package beanstalk wrappers beanstalk client as a broker.
package beanstalk

import (
	"bufio"
	"git.oschina.net/chaos.su/beanstalkc"
	"net"
)

type BrokerBeanstalk struct {
	beanstalkc.Client
}

func New(hostAddr string) (br *BrokerBeanstalk, err error) {
	conn, err := net.Dial("tcp", hostAddr)
	if err != nil {
		return
	}
	r := bufio.NewReaderSize(conn, 102400)
	w := bufio.NewWriter(conn)
	br = &BrokerBeanstalk{}
	br.Client = beanstalkc.Client{
		Conn:       conn,
		ReadWriter: bufio.NewReadWriter(r, w),
	}
	return
}

func (br *BrokerBeanstalk) Pub(priority uint32, delay, ttr uint64, data []byte) (jobId uint64, err error) {
	jobId, _, err = br.Put(priority, delay, ttr, data)
	return
}

func (br *BrokerBeanstalk) ListTopics() (topics []string, err error) {
	topics, err = br.ListTubes()
	return
}

func (br *BrokerBeanstalk) StatsTopic(topicName string) (stats map[string]interface{}, err error) {
	stats, err = br.StatsTube(topicName)
	return
}

func (br *BrokerBeanstalk) Release(jobId uint64, priority uint32, delay uint64) (err error) {
	_, err = br.Client.Release(jobId, priority, delay)
	return
}

func (br *BrokerBeanstalk) Close() {
	br.Conn.Close()
}

func (br *BrokerBeanstalk) UnWatch(topicName string)(err error){
	_, err = br.Ignore(topicName)
	return
}

