package rpc

func RegisterHandlerRegister(tag string, myHandler Handler){
	handlerContainer.Register(tag, myHandler)
}

func GetHandlerContainer() *Handlers{
	return &handlerContainer
}