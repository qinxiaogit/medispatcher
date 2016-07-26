package beanstalk

type MsgStats struct {
	QueueName string `json:"tube"`
	Pri uint32 `json:"pri"`
	Delay uint64 `json:"delay"`
	TTR   uint64 `json:"ttr"`
}
type Msg struct {
	Id uint64
	Body []byte
	QueueServer string
	Stats MsgStats
}
