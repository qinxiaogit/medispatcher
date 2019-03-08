// Package strutil contains utilities for operations on strings.

package strutil

import "strconv"

func UpperFirst(str string) string {
	if len(str) < 1 || str[0] < 'a' || str[0] > 'z' {
		return str
	}

	strB := []byte(str)
	strB[0] = strB[0] - byte(32)
	return string(strB)
}

func LowerFirst(str string) string {
	if len(str) < 1 || str[0] < 'A' || str[0] > 'Z' {
		return str
	}

	strB := []byte(str)
	strB[0] = strB[0] + byte(32)
	return string(strB)
}

func ToInt(i interface{}) int {
	switch v := i.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float32:
		return int(v)
	case float64:
		return int(v)
	case string:
		vv, _ := strconv.Atoi(v)
		return vv
	default:
		return 0
	}
}
