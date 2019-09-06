package downloader

import (
	"time"

	"github.com/divinerapier/oss-benchmark/args"
	"github.com/divinerapier/oss-benchmark/downloader/aliyunoss"
	"github.com/divinerapier/oss-benchmark/downloader/s3"
)

const (
	AliOss = aliyunoss.AliOss
	AwsS3  = s3.Aws
	CephS3 = s3.Ceph
)

type Downloader interface {
	StatTicker(time.Duration)
	Start()
}

func NewDownloader(arg args.Args) Downloader {
	switch arg.Provider {
	case AliOss:
		return aliyunoss.NewDownloader(arg)
	case AwsS3, CephS3:
		return s3.NewDownloader(arg)
	default:
		panic("unknown provider")
	}
}
