// Package sendproxy provides transfer proxies from sending message to subscribers.
package http

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"strings"
	"time"
	"io"
	"compress/gzip"
	"fmt"
	"sync"
	"encoding/json"
)

type clientInfo struct {
	sync.RWMutex
	httpClient *http.Client
	transport *http.Transport
	lastAccessTime time.Time
}

type clientPool struct{
	sync.RWMutex
	pools map[string]*clientInfo
}

func (p *clientPool) get(addr string, reqTimeout time.Duration)(*http.Client, error){
	destUrl, err := url.Parse(addr)
	if err != nil {
		return nil , err
	}
	clientKey := fmt.Sprintf("%v[%v]", destUrl.Host,reqTimeout.Nanoseconds())
	p.RLock()
	if _, exists := p.pools[clientKey]; exists{
		c := p.pools[clientKey]
		p.RUnlock()
		c.Lock()
		c.lastAccessTime = time.Now()
		c.Unlock()
		return c.httpClient, nil
	}
	p.RUnlock()
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	client := &http.Client{
		Timeout: reqTimeout,
		Transport: transport,
	}
	newClientInfo := &clientInfo{
		httpClient: client,
		transport: transport,
		lastAccessTime: time.Now(),
	}
	p.Lock()
	p.pools[clientKey] = newClientInfo
	p.Unlock()
	return newClientInfo.httpClient, nil
}

var clients = clientPool{
	pools: map[string]*clientInfo{},
}

func init(){
	go func(){
		ticker := time.NewTicker(time.Second * 10)
		for{
			<-ticker.C
			tn := time.Now()
			clients.Lock()
			for key, c := range clients.pools {
				c.Lock()
				if tn.Sub(c.lastAccessTime) > time.Second * 10 {
					c.transport.CloseIdleConnections()
					delete(clients.pools, key)
				}
				c.Unlock()
			}
			clients.Unlock()
		}
	}()
}

func TransferJSON(addr string, data map[string]string, timeout time.Duration) (httpStatusCode int, respData []byte, err error) {
	var req *http.Request
	reqData, err := json.Marshal(data)
	if err != nil {
		return 500, nil, err
	}
	req, err = http.NewRequest("POST", addr, strings.NewReader(string(reqData)))
	fmt.Println(string(reqData))
	if err != nil {
		return
	}
	var client *http.Client
	client, err = clients.get(addr, timeout)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "MEDipatcher/2.0.0-alpha")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "gzip")
	//req.Header.Set("Connection", "close")
//	req.Header.Set("Keep-Alive", "300")
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, _ = gzip.NewReader(resp.Body)
	default:
		reader = resp.Body
	}
	defer reader.Close()
	httpStatusCode = resp.StatusCode
	buffLen := 20
	buff := make([]byte, buffLen)
	for {
		n, e := reader.Read(buff)
		if n > 0 {
			respData = append(respData, buff[0:n]...)
		}
		if e != nil {
			break
		}
	}
	return
}

// Transfer acts a http post request.
// TODO: keep-alive on connection pool
func Transfer(addr string, data map[string]string, timeout time.Duration) (httpStatusCode int, respData []byte, err error) {
	reqData := url.Values{}
	for k, v := range data {
		reqData[k] = []string{v}
	}
	var req *http.Request
	req, err = http.NewRequest("POST", addr, strings.NewReader(reqData.Encode()))
	if err != nil {
		return
	}
	var client *http.Client
	client, err = clients.get(addr, timeout)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "MEDipatcher/2.0.0-alpha")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "gzip")
	//req.Header.Set("Connection", "close")
//	req.Header.Set("Keep-Alive", "300")
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, _ = gzip.NewReader(resp.Body)
	default:
		reader = resp.Body
	}
	defer reader.Close()
	httpStatusCode = resp.StatusCode
	buffLen := 20
	buff := make([]byte, buffLen)
	for {
		n, e := reader.Read(buff)
		if n > 0 {
			respData = append(respData, buff[0:n]...)
		}
		if e != nil {
			break
		}
	}
	return
}
