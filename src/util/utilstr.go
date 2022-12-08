package util

import (
	"strings"
)

// input:
//        s := [1234, 'Henry', 'Shanghai']
// call:
// 	      sliceFirst(s, ",")
// output:
// 		  1234
func sliceFirst(s string, sep string) string {
	ret := ""
	index := strings.Index(s, sep)
	if (index > -1) {
		ret = s[1:index]
	}

	return ret
}
