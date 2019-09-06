package aliyunoss

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
	"github.com/divinerapier/oss-benchmark/args"
	"github.com/divinerapier/oss-benchmark/stat"
	"github.com/golang/glog"
)

type OfficialDownloader struct {
	arg        args.Args
	bucket     *oss.Bucket
	objectList chan string
	options    []oss.Option
	stat       stat.Statistics
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

func NewOfficialDownloader(arg args.Args, options ...oss.Option) *OfficialDownloader {
	var wg sync.WaitGroup
	if err := arg.Validate(); err != nil {
		panic(err)
	}
	objectList := make(chan string, 1024)
	file, err := os.Open(arg.InputFile)
	if err != nil {
		panic(err)
	}

	downloader := &OfficialDownloader{
		arg:        arg,
		objectList: objectList,
		options:    options,
		wg:         &wg,
	}

	err = downloader.GetTestBucket(arg)
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
			if arg.Prefix != "" {
				object = filepath.Join(arg.Prefix, object)
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
	for i := 0; i < d.arg.Threads; i++ {
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
func (d *OfficialDownloader) GetTestBucket(arg args.Args) error {
	// New client
	client, err := oss.New(arg.Endpoint, arg.AccessKey, arg.SecretKey)
	if err != nil {
		return fmt.Errorf("connect to oss. error: %v", err)
	}

	// Get bucket
	bucket, err := client.Bucket(arg.Bucket)
	if err != nil {
		return fmt.Errorf("get bucket. error: %v", err)
	}
	d.bucket = bucket
	return nil
}
