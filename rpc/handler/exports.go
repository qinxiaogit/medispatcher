package handler

func RegisterHandlerRegister(tag string, myHandler Handler){
	handlerContainer.Register(tag, myHandler)
}

func GetContainer() *Handlers{
	return &handlerContainer
}