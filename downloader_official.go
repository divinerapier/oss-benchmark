package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/golang/glog"
)

type OfficialDownloader struct {
	args       Args
	bucket     *oss.Bucket
	objectList chan string
	options    []oss.Option
	stat       Statistics
	wg         *sync.WaitGroup
}

// GetObjectSample shows the streaming download, range download and resumable download.
func (d *OfficialDownloader) GetObject(key string) (int64, error) {
	resp, err := d.bucket.GetObject(key, d.options...)
	if err != nil {
		return 0, err
	}
	defer resp.Close()
	return io.Copy(ioutil.Discard, resp)
}

func (d *OfficialDownloader) Stat() string {
	return d.stat.Stat()
}

func (s *OfficialDownloader) StatTicker(tick time.Duration) {
	ticker := time.NewTicker(tick)
	for range ticker.C {
		fmt.Println(s.Stat())
	}
}

func NewOfficialDownloader(args Args, options ...oss.Option) *OfficialDownloader {
	var wg sync.WaitGroup
	if err := args.Validate(); err != nil {
		panic(err)
	}
	objectList := make(chan string, 1024)
	file, err := os.Open(args.InputFile)
	if err != nil {
		panic(err)
	}

	downloader := &OfficialDownloader{
		args:       args,
		objectList: objectList,
		options:    options,
		wg:         &wg,
	}

	err = downloader.GetTestBucket(args)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	wg.Add(1)
	go func() {
		for scanner.Scan() {
			object := scanner.Text()
			if object == "" {
				continue
			}
			keys := strings.Split(object, " ")
			if len(keys) > 0 && keys[0] != "" {
				object = keys[0]
			}
			if object == "" {
				continue
			}
			if args.Prefix != "" {
				object = filepath.Join(args.Prefix, object)
			}
			objectList <- object
		}
		close(objectList)
		wg.Done()
	}()

	return downloader
}

func (d *OfficialDownloader) Start() {
	d.stat.Start()
	defer d.wg.Wait()
	var wg sync.WaitGroup
	for i := 0; i < d.args.Threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for object := range d.objectList {
				size, err := d.GetObject(object)
				if err != nil {
					glog.Errorf("failed to download object. key: %s, error: %v", object, err)
					d.stat.AddFailedCount(1)
					d.stat.AddFailedSize(size)
					continue
				}
				d.stat.AddSuccessCount(1)
				d.stat.AddSuccessSize(size)
			}
		}()
	}
	wg.Wait()
}

// GetTestBucket creates the test bucket
func (d *OfficialDownloader) GetTestBucket(args Args) error {
	// New client
	client, err := oss.New(args.Endpoint, args.AccessKey, args.SecretKey)
	if err != nil {
		return fmt.Errorf("connect to oss. error: %v", err)
	}

	// Get bucket
	bucket, err := client.Bucket(args.Bucket)
	if err != nil {
		return fmt.Errorf("get bucket. error: %v", err)
	}
	d.bucket = bucket
	return nil
}
