package testutils

import (
	"net"
	"strconv"
)

func GetPort(addr string) int {
	if _, portStr, err := net.SplitHostPort(addr); err == nil {
		if p, err := strconv.Atoi(portStr); err == nil {
			return p
		}
	}
	return 0
}
