package data

import (
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"github.com/qinxiaogit/medispatcher/strutil"
	"reflect"
	"strconv"
)

func UnserializeMessage(dataB []byte) (msg *MessageStuct, err error) {
	var dataD map[string]interface{}
	switch MSG_SERIALIZER {
	case "msgpack":
		err = msgpack.Unmarshal(dataB, &dataD)
	}
	msg = new(MessageStuct)
	if err == nil {
		rf := reflect.ValueOf(msg).Elem()
		for field, value := range dataD {
			field = strutil.UpperFirst(field)
			// Hot fixing for type
			switch field {
				case "OriginJobId","LogId", "RetryTimes":
					value, err = strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
				case "Time":
					value, err = strconv.ParseFloat(fmt.Sprintf("%v", value), 64)
			}

			msgField := rf.FieldByName(field)
			if msgField.IsValid() {
				switch msgField.Kind() {
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					switch value.(type) {
					case int64:
						msgField.SetUint(uint64(value.(int64)))
					case uint64:
						msgField.SetUint(value.(uint64))
					}
				case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					switch value.(type) {
					case int64:
						msgField.SetInt(value.(int64))
					case uint64:
						msgField.SetInt(int64(value.(uint64)))
					}
				default:
					if field == "Body" {
						switch value.(type) {
						case map[interface{}]interface{}:
							value = FixMsgpackMap(value.(map[interface{}]interface{}))
						case []interface{}:
							lItf := value.([]interface{})
							for i, elem := range lItf {
								if tElem, ok := elem.(map[interface{}]interface{}); ok {
									lItf[i] = FixMsgpackMap(tElem)
								}
							}
							value = lItf
						}
					}
					if value != nil {
						msgField.Set(reflect.ValueOf(value))
					}
				}
			}
		}
	}
	return
}

func SerializeMessage(msg MessageStuct) (data []byte, err error) {
	switch MSG_SERIALIZER {
	case "msgpack":
		data, err = msgpack.Marshal(msg)
	}
	return
}

func FixMsgpackMap(valueI map[interface{}]interface{}) map[string]interface{} {
	fixType := reflect.TypeOf(valueI)
	value := map[string]interface{}{}
	for f, v := range valueI {
		if reflect.TypeOf(v) == fixType {
			v = FixMsgpackMap(v.(map[interface{}]interface{}))
		} else if lItf, ok := v.([]interface{}); ok {
			for i, elem := range lItf {
				if reflect.TypeOf(elem) == fixType {
					lItf[i] = FixMsgpackMap(elem.(map[interface{}]interface{}))
				}
			}
			v = lItf
		}
		value[fmt.Sprintf("%v", f)] = v
	}
	return value
}
