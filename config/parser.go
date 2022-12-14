package config

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io/ioutil"
	"github.com/qinxiaogit/medispatcher/Alerter"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	toml "git.oschina.net/chaos.su/go-toml"
)

var flags = flag.NewFlagSet("medispatcher", flag.ExitOnError)

var configFile string

// GetConfigPath 得到配置文件路径
// 该方法必须在ParseConfig方法之后调用
func GetConfigPath() string {
	return configFile
}

func GetConfig() Config {
	if config == nil {
		panic("config not intialized, you should call config.Setup() first!")
	}
	return *config
}

func GetConfigPointer() *Config {
	if config == nil {
		panic("config not intialized, you should call config.Setup() first!")
	}
	return config
}

func GetFlags() *flag.FlagSet {
	return flags
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

// ParseConfig 解析配置文件/etc/medispatcher.toml
func ParseConfig() (*Config, error) {
	var defaultConfigFile string
	var err error
	if runtime.GOOS == "windows" {
		clientPath, err := filepath.Abs(os.Args[0])
		if err != nil {
			return nil, fmt.Errorf("Failed to get client ABS path: %v", err)
		}
		defaultConfigFile = path.Dir(clientPath) + string(os.PathSeparator) + "config.toml"
	} else {
		defaultConfigFile = "/etc/medispatcher.toml"
	}
	var showHelp, showVersion bool
	flags.StringVar(&configFile, "f", defaultConfigFile, "path to the medispatcher config file.")
	flags.BoolVar(&showHelp, "h", false, "show help message.")
	flags.BoolVar(&showHelp, "help", false, "show help message.")
	flags.BoolVar(&showVersion, "version", false, "show version information.")

	args := []string{}
	if len(os.Args) > 1 {
		args = os.Args[1:]
	}
	flags.Parse(args)
	if showHelp {
		flags.PrintDefaults()
	}

	if showVersion {
		fb, err := ioutil.ReadFile(os.Args[0])
		if err == nil {
			filehash := fmt.Sprintf("%x", md5.Sum(fb))
			fmt.Printf("medispatcher %v(%v) build with %s on %s git-hash %s \r\n", VerNo, filehash, runtime.Version(), strings.Replace(BuildTime, "_", " ", -1), GitHash)
		} else {
			fmt.Printf("medispatcher %v with %s on build on %s\r\n", VerNo, runtime.Version(), strings.Replace(BuildTime, "_", " ", -1))
		}
	}

	if showHelp || showVersion {
		os.Exit(0)
	}
	if len(os.Args) == 2 && strings.Index(os.Args[1], "-") != 0 {
		configFile = os.Args[1]
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
				switch key {
				case "AlerterEmail", "AlerterSms", "AlarmPlatform":
					var alerterType string
					if key == "AlerterEmail" {
						alerterType = "Email"
					} else if key == "AlerterSms" {
						alerterType = "Sms"
					} else {
						alerterType = "AlarmPlatform"
					}
					cCfg := Alerter.Config{
						ProxyType: alerterType,
					}

					cCfgRf := reflect.ValueOf(&cCfg)
					for cKey, cValue := range TraverseTomlTree(sItem.(*toml.TomlTree)) {
						cElem := cCfgRf.Elem().FieldByName(cKey)
						if cElem.IsValid() {
							if cKey == "PostFieldsMap" {
								mV := map[string]string{}
								for k, v := range cValue.(map[string]interface{}) {
									mV[k] = v.(string)
								}
								cElem.Set(reflect.ValueOf(mV))
							} else {
								cElem.Set(reflect.ValueOf(cValue))
							}
						}
					}
					elem.Set(reflect.ValueOf(cCfg))
				default:
					elem.Set(reflect.ValueOf(TraverseTomlTree(sItem.(*toml.TomlTree))))
				}
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
			default:
				elem.Set(reflect.ValueOf(sItem))
			}
		}
	}
	return &parsedConfigs, nil
}
