// Package sendproxy provides transfer proxies from sending message to subscribers.
package http

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Transfer acts a http post request.
// TODO: keep-alive on connection pool
func Transfer(addr string, data map[string]string, timeout time.Duration) (httpStatusCode int, respData []byte, err error) {
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Timeout: timeout, Transport: tr}
	reqData := url.Values{}
	for k, v := range data {
		reqData[k] = []string{v}
	}
	req, _ := http.NewRequest("POST", addr, strings.NewReader(reqData.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "MEDipatcher/2.0.0-alpha")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Connection", "close")
//	req.Header.Set("Keep-Alive", "300")
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	httpStatusCode = resp.StatusCode
	buffLen := 20
	buff := make([]byte, buffLen)
	for {
		n, e := resp.Body.Read(buff)
		if n > 0 {
			respData = append(respData, buff[0:n]...)
		}
		if e != nil {
			break
		}
	}
	return
}
