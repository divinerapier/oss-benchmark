package main

import (
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type Provider string

const (
	AliOss = "ali-oss"
	AwsS3  = "aws-s3"
	CephS3 = "ceph-s3"
)

type Downloader interface {
	StatTicker(time.Duration)
	Start()
}

func NewDownloader(args Args, options ...oss.Option) Downloader {
	if args.Official {
		return NewOfficialDownloader(args, options...)
	}
	return NewUnofficialDownloader(args)
}
