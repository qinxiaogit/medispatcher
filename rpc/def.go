package rpc

import (
	"fmt"
)

type Handler interface {
	Process(args map[string]interface{}) (interface{}, error)
}

type Handlers struct {
	rwLocker           chan uint8
	registeredHandlers map[string]Handler
}

var handlerContainer = Handlers{
	rwLocker:           make(chan uint8, 1),
	registeredHandlers: map[string]Handler{},
}

func (h *Handlers) lock() {
	(*h).rwLocker <- 1
}

func (h *Handlers) unlock() {
	<-(*h).rwLocker
}

func (h *Handlers) Register(tag string, myHandler Handler) {
	(*h).registeredHandlers[tag] = myHandler
}

func (h *Handlers) Exists(tag string) bool {
	if _, exists := (*h).registeredHandlers[tag]; exists {
		return true
	}
	return false
}

func (h *Handlers) Run(tag string, args map[string]interface{}) (interface{}, error) {
	if !h.Exists(tag) {
		return nil, ErrorHandleNotExists{fmt.Sprintf("Cmd handler \"%s\" not exists!", tag)}
	}
	return (*h).registeredHandlers[tag].Process(args)
}

func (h *Handlers) GetRegisteredHandlerList() []string {
	var hList []string
	for n, _ := range (*h).registeredHandlers {
		hList = append(hList, n)
	}
	return hList
}

type ErrorHandleNotExists struct {
	Message string
}

func (e ErrorHandleNotExists) Error() string {
	return e.Message
}
