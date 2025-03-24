package utils

import "runtime"

var IntSize = 8

func init() {
	if runtime.GOARCH == "386" || runtime.GOARCH == "arm" {
		IntSize = 4
	}
}
