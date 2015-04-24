// Package strutil contains utilities for operations on strings.

package strutil

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
