package config

import (
	"errors"
	"fmt"
	toml "git.oschina.net/chaos.su/go-toml"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
)

func GetConfig() Config {
	if config == nil {
		panic("config not intialized, you should call config.Setup() first!")
	}
	return *config
}

func TraverseTomlTree(t *toml.TomlTree) map[string]interface{} {
	keys := t.Keys()
	mm := make(map[string]interface{}, 1)
	for _, k := range keys {
		elm := t.Get(k)
		switch tp := elm.(type) {
		case *toml.TomlTree:
			mm[k] = TraverseTomlTree(tp)
		case nil:
		default:
			mm[k] = elm
		}
	}
	return mm
}

// 解析配置文件/etc/medispatcher.toml
func ParseConfig() (*Config, error) {
	var configFile string
	var err error
	if runtime.GOOS == "windows" {
		clientPath, err := filepath.Abs(os.Args[0])
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to get client ABS path: %v", err))
		}
		configFile = path.Dir(clientPath) + string(os.PathSeparator) + "config.toml"
	} else {
		configFile = "/etc/medispatcher.toml"
	}

	configTree, err := toml.LoadFile(configFile)
	if err != nil {
		return nil, err
	}

	parsedConfigs := *config
	cr := reflect.ValueOf(&parsedConfigs)
	for _, key := range configTree.Keys() {
		elem := cr.Elem().FieldByName(key)
		if elem.IsValid() {
			sItem := configTree.Get(key)
			switch sItem.(type) {
			case string:
				svItem := sItem.(string)
				if svItem != "" {
					elem.SetString(svItem)
				}
			case *toml.TomlTree:
				elem.Set(reflect.ValueOf(TraverseTomlTree(sItem.(*toml.TomlTree))))
			case int64:
				switch elem.Kind() {
				case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
					elem.SetInt(sItem.(int64))
				case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
					elem.SetUint(uint64(sItem.(int64)))
				}
			case uint64:
				switch elem.Kind() {
				case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
					elem.SetInt(int64(sItem.(int64)))
				case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
					elem.SetUint(sItem.(uint64))
				}
			}
		}
	}
	return &parsedConfigs, nil
}
