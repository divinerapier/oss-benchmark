package main

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type ProgressListener struct {
}

func (ln *ProgressListener) ProgressChanged(event *oss.ProgressEvent) {

}
