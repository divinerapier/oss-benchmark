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

	"github.com/denverdino/aliyungo/oss"
	"github.com/golang/glog"
)

type UnofficialDownloader struct {
	args       Args
	bucket     *oss.Bucket
	objectList chan string
	stat       Statistics
	wg         *sync.WaitGroup
}

func NewUnofficialDownloader(args Args) *UnofficialDownloader {
	var wg sync.WaitGroup
	if err := args.Validate(); err != nil {
		panic(err)
	}
	objectList := make(chan string, 1024)
	file, err := os.Open(args.InputFile)
	if err != nil {
		panic(err)
	}

	downloader := &UnofficialDownloader{
		args:       args,
		objectList: objectList,
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

// GetObjectSample shows the streaming download, range download and resumable download.
func (d *UnofficialDownloader) GetObject(key string) (int64, error) {
	resp, err := d.bucket.GetResponse(key)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return io.Copy(ioutil.Discard, resp.Body)
}

func (d *UnofficialDownloader) Stat() string {
	return d.stat.Stat()
}

func (s *UnofficialDownloader) StatTicker(tick time.Duration) {
	ticker := time.NewTicker(tick)
	for range ticker.C {
		fmt.Println(s.Stat())
	}
}

func (d *UnofficialDownloader) Start() {
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
func (d *UnofficialDownloader) GetTestBucket(args Args) error {
	// New client
	client := oss.NewOSSClient(d.fromEndpoint(args.Endpoint), d.isInternal(args.Endpoint), args.AccessKey, args.SecretKey, false)
	// Get bucket
	d.bucket = client.Bucket(args.Bucket)
	return nil
}

func (d *UnofficialDownloader) fromEndpoint(endpoint string) oss.Region {
	switch {
	case strings.Contains(endpoint, "hangzhou"):
		return oss.Hangzhou
	case strings.Contains(endpoint, "qingdao"):
		return oss.Qingdao
	case strings.Contains(endpoint, "beijing"):
		return oss.Beijing
	case strings.Contains(endpoint, "hongkong"):
		return oss.Hongkong
	case strings.Contains(endpoint, "shenzhen"):
		return oss.Shenzhen
	case strings.Contains(endpoint, "shanghai"):
		return oss.Shanghai
	case strings.Contains(endpoint, "zhangjiakou"):
		return oss.Zhangjiakou
	case strings.Contains(endpoint, "huhehaote"):
		return oss.Huhehaote

	case strings.Contains(endpoint, "west-1"):
		return oss.USWest1
	case strings.Contains(endpoint, "east-1"):
		return oss.USEast1
	case strings.Contains(endpoint, "southeast-1"):
		return oss.APSouthEast1
	case strings.Contains(endpoint, "northeast-1"):
		return oss.APNorthEast1
	case strings.Contains(endpoint, "southeast-2"):
		return oss.APSouthEast2

	case strings.Contains(endpoint, "me-east-1"):
		return oss.MEEast1
	case strings.Contains(endpoint, "eu-central-1"):
		return oss.EUCentral1
	case strings.Contains(endpoint, "eu-west-1"):
		return oss.EUWest1
	default:
		return oss.DefaultRegion
	}
}

func (d *UnofficialDownloader) isInternal(endpoint string) bool {
	return strings.Contains(endpoint, "internal")
}
