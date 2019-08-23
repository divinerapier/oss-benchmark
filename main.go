package main

import (
	"flag"
	"time"
)

func main() {
	flag.Parse()
	downloader := NewDownloader(args)
	go downloader.StatTicker(time.Duration(args.SampleInterval) * time.Millisecond)
	downloader.Start()
}
