// Package cache provides simple in-memory key=>value caches. no ttl facilities, you have to remove items manually.
package cache

import (
	"strings"
	"sync"
)

var caches  map[string]interface{}

var rwLock = &sync.Mutex{}

func init(){
	caches = map[string]interface{}{}
}


func Set(key string, data interface{}) {
	rwLock.Lock()
	defer rwLock.Unlock()
	caches[key] = data
}

func Get(key string) (interface{}, bool) {
	rwLock.Lock()
	defer rwLock.Unlock()
	data, ok := caches[key]
	return data, ok
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
	rwLock.Lock()
	caches = map[string]interface{}{}
	rwLock.Unlock()
	return true
}

func DeleteByPrefix(prefixes []string)bool{
	rwLock.Lock()
	defer rwLock.Unlock()
	for key, _ := range caches {
		for _, prefix := range prefixes {
			if strings.Index(key, prefix) == 0 {
				delete(caches, key)
			}
		}
	}
	return true
}
