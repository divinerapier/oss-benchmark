package main

import (
	"errors"
	"flag"
	"runtime"
)

type Args struct {
	Endpoint       string
	AccessKey      string
	SecretKey      string
	Bucket         string
	InputFile      string
	Prefix         string
	Threads        int
	SampleInterval int
	Official       bool
}

func (a Args) Validate() error {
	if a.Endpoint == "" {
		return errors.New("missing endpoint")
	}
	if a.AccessKey == "" {
		return errors.New("missing access key")
	}
	if a.SecretKey == "" {
		return errors.New("missing secret key")
	}
	if a.Bucket == "" {
		return errors.New("missing bucket")
	}
	if a.InputFile == "" {
		return errors.New("missing input file")
	}
	return nil
}

var args Args

func init() {
	flag.StringVar(&args.Endpoint, "endpoint", "", "oss endpoint")
	flag.StringVar(&args.AccessKey, "access-key", "", "oss access key")
	flag.StringVar(&args.SecretKey, "secret-key", "", "oss secret key")
	flag.StringVar(&args.Bucket, "bucket", "", "oss bucket")
	flag.StringVar(&args.InputFile, "file", "", "file contains the object list to test")
	flag.StringVar(&args.Prefix, "prefix", "", "prefix of object key")
	flag.IntVar(&args.Threads, "thread", runtime.NumCPU(), "threads number")
	flag.IntVar(&args.SampleInterval, "sample-interval", 1000, "sample interval in ms")
	flag.BoolVar(&args.Official, "official", true, "use official oss sdk")
}
