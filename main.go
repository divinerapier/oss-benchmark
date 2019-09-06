package main

import (
	"time"

	"github.com/divinerapier/oss-benchmark/args"
	"github.com/divinerapier/oss-benchmark/downloader"
)

func main() {
	arg := args.ParseArguments()
	dl := downloader.NewDownloader(arg)
	go dl.StatTicker(time.Duration(arg.SampleInterval) * time.Millisecond)
	dl.Start()
}
