package rpclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	// 127.0.0.1 5601
	addr string
	conn net.Conn
}

func (c *Client) connect() error {
	conn, err := net.Dial("tcp", (*c).addr)
	if err != nil {
		return err
	}
	(*c).conn = conn
	return nil
}

func (c *Client) Call(name string, args map[string]interface{}) (re interface{}, err error) {
	reqData := map[string]interface{}{"cmd": name, "args": args}
	var reqB []byte
	reqB, err = c.packData(reqData)
	if err != nil {
		return
	}
	_, err = (*c).conn.Write(reqB)
	if err != nil {
		return
	}

	respHeadTag := make([]byte, 16)
	var count, start int
	for start < 16 {
		(*c).conn.SetReadDeadline(time.Now().Add(NET_READ_TIMEOUT))
		count, err = (*c).conn.Read(respHeadTag[start:])
		if err == nil {
			start += count
		} else {
			err = errors.New(fmt.Sprintf("Data length Read error, connection may closed unexpectedly:%s\nAlread read(%d) bytes", err, start))
			break
		}
	}
	if err != nil {
		return
	}

	bodyLen, _ := strconv.Atoi(strings.Trim(string(respHeadTag[0:7]), "\000"))
	status := strings.Trim(string(respHeadTag[7:]), "\000")
	respData := make([]byte, bodyLen)
	start, count = 0, 0
	for {
		if start == bodyLen {
			break
		}
		(*c).conn.SetReadDeadline(time.Now().Add(NET_READ_TIMEOUT))
		count, err = (*c).conn.Read(respData[start:])
		if err == nil {
			start += count
		} else {
			err = errors.New(fmt.Sprintf("Read error:%s", err))
			break
		}
	}
	if err != nil {
		return
	}
	if status != "ok" {
		re = nil
		err = errors.New(fmt.Sprintf("handler failed: %s", respData))
		return
	}
	err = json.Unmarshal(respData, &re)
	return
}

func (c *Client) packData(data map[string]interface{}) ([]byte, error) {
	bStr, err := json.Marshal(data)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to pack data: %v", err))
	}
	headTag := []byte(strconv.Itoa(len(bStr)))
	headTag = append(headTag, make([]byte, 8-len(headTag))...)
	return append(headTag, bStr...), nil
}

func New(addr string) (*Client, error) {
	c := &Client{addr: addr}
	err := c.connect()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client)Close()error{
	return (*c).conn.Close()
}