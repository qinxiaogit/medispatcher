package handlers

import(
	"medispatcher/rpc/handler"
	"medispatcher/data/cache"
	"errors"
)

type ClearDataCache struct{}

func init(){
	handler.RegisterHandlerRegister("ClearDataCache", ClearDataCache{})
}

// Clears caches of data package in process memory.
// args  cache key prefixes.
func (_ ClearDataCache)Process(args map[string]interface{})(interface{}, error){
	if len(args) < 1 {
		return cache.Flush(),nil
	} else {
		var prefixes []string
		var ok bool
		for _, i:= range args {
			prefixes, ok = i.([]string)
			if !ok{
			   return nil, errors.New("prefixes is not type string list.")
			}
		}
		return cache.DeleteByPrefix(prefixes), nil
	}
}