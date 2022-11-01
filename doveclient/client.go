package doveclient

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
)

func NewDoveClient(address string) *doveClient {
	return &doveClient{
		address: address,
	}
}

type doveClient struct {
	address string
}

func (dc *doveClient)Call(methodName string,args map[string]interface{}) (string,[]byte,error) {
	var clientData map[string]interface{}

	clientData["args"] = args
	clientData["cmd"]  = methodName

	conn ,err := net.Dial("unix",dc.address)
	if err != nil{
		fmt.Println(err)
		panic(err)
	}
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {

		}
	}(conn)
	clientDataJs ,err := json.Marshal(clientData)
	lenStr := fmt.Sprintf("%08d",len(clientDataJs))

	conn.Write([]byte(lenStr))

	conn.Write(clientDataJs)

	b, err := ioutil.ReadAll(conn)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("333",string(b))
	return "ok",b,err

}

func (dc *doveClient)Close()  {

}
