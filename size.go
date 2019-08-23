package main

import (
	"fmt"
	"strconv"
)

type Size int64

func (s Size) String() string {
	if s < 1024 {
		return strconv.FormatInt(int64(s), 10) + "B"
	}
	tempS := float64(s)
	tempS /= 1024
	if tempS < 1024 {
		return fmt.Sprintf("%.2fKB", tempS)
	}
	tempS /= 1024
	if tempS < 1024 {
		return fmt.Sprintf("%.2fMB", tempS)
	}
	tempS /= 1024
	if tempS < 1024 {
		return fmt.Sprintf("%.2fGB", tempS)
	}
	return fmt.Sprintf("%.2fTB", tempS/1024)
}
