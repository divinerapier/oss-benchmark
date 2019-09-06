package aliyunoss

import (
	"time"

	"github.com/divinerapier/oss-benchmark/args"
)

const AliOss = "aliyun-oss"

type Downloader struct {
	official   *OfficialDownloader
	unofficial *UnofficialDownloader
}

func NewDownloader(arg args.Args) *Downloader {
	if arg.Official {
		return &Downloader{
			official: NewOfficialDownloader(arg),
		}
	}
	return &Downloader{
		unofficial: NewUnofficialDownloader(arg),
	}
}

func (d *Downloader) StatTicker(tick time.Duration) {
	if d.official != nil {
		d.official.StatTicker(tick)
	} else {
		d.unofficial.StatTicker(tick)
	}
}
func (d *Downloader) Start() {
	if d.official != nil {
		d.official.Start()
	} else {
		d.unofficial.Start()
	}
}
