// Package cache provides simple in-memory key=>value caches. no ttl facilities, you have to remove items manually.
package cache

import (
	"strings"
)

var caches  map[string]interface{}

var cacheRWLock = make(chan int8, 1)

func init(){
	caches = map[string]interface{}{}
}

func getRWLock() {
	cacheRWLock <- int8(1)
}

func releaseRWLock() {
	<-cacheRWLock
}

func Set(key string, data interface{}) {
	getRWLock()
	defer releaseRWLock()
	caches[key] = data
}

func Get(key string) interface{} {
	getRWLock()
	defer releaseRWLock()
	data := caches[key]
	return data
}

func Exists(key string) bool {
	var exists bool
	_, exists = caches[key]
	return exists
}

func Delete(key string) {
	delete(caches, key)
}

func Flush()bool{
	getRWLock()
	caches = map[string]interface{}{}
	releaseRWLock()
	return true
}

func DeleteByPrefix(prefixes []string)bool{
	getRWLock()
	defer releaseRWLock()
	for key, _ := range caches {
		for _, prefix := range prefixes {
			if strings.Index(key, prefix) == 0 {
				delete(caches, key)
			}
		}
	}
	return true
}
