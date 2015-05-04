package handlers

import(
	"medispatcher/rpc"
	"medispatcher/data/cache"
	"errors"
	"reflect"
)

type ClearDataCache struct{}

func init(){
	rpc.RegisterHandlerRegister("ClearDataCache", ClearDataCache{})
}

// Clears caches of data package in process memory.
// args  cache key prefixes.
func (_ ClearDataCache)Process(args map[string]interface{})(interface{}, error){
	if len(args) < 1 {
		return cache.Flush(),nil
	} else {
		var prefixes []string
		var ok bool
		var tPre []interface{}
		var tPreS string
		for _, i:= range args {
			tPre, ok = i.([]interface{})
			if !ok{
			   return nil, errors.New("prefixes '"+reflect.TypeOf(i).String()+"' is not type interface list.")
			}
			for _, i:=range tPre {
				tPreS, ok= i.(string)
				if !ok{
					return nil, errors.New("prefix is not a string.")
				} else {
					prefixes = append(prefixes, tPreS)
				}
			}
		}
		return cache.DeleteByPrefix(prefixes), nil
	}
}