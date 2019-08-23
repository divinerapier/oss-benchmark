package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/golang/glog"
)

type Size int64

func (s Size) String() string {
	if s < 1024 {
		return strconv.FormatInt(int64(s), 10) + "B"
	}
	tempS := float64(s)
	tempS /= 1024
	if tempS < 1024 {
		return fmt.Sprintf("%.2fKB", tempS)
	}
	tempS /= 1024
	if tempS < 1024 {
		return fmt.Sprintf("%.2fMB", tempS)
	}
	tempS /= 1024
	if tempS < 1024 {
		return fmt.Sprintf("%.2fGB", tempS)
	}
	return fmt.Sprintf("%.2fTB", tempS/1024)
}

type Args struct {
	Endpoint       string
	AccessKey      string
	SecretKey      string
	Bucket         string
	InputFile      string
	Prefix         string
	Threads        int
	SampleInterval int
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
}

type Downloader struct {
	args       Args
	bucket     *oss.Bucket
	objectList chan string
	options    []oss.Option
	stat       Statistics
	wg         *sync.WaitGroup
}

type Statistics struct {
	StartAt          time.Time
	Count            int64
	TotalSize        int64
	SuccessCount     int64
	SuccessSize      int64
	FailedCount      int64
	FailedSize       int64
	LastTotalCount   int64
	LastTotalSize    int64
	LastSuccessCount int64
	LastSuccessSize  int64
	Speed            string
	SuccessSpeed     string
}

func (s *Statistics) Start() {
	s.StartAt = time.Now()
}

func (s *Statistics) AddSuccessCount(count int64) {
	atomic.AddInt64(&s.SuccessCount, count)
	atomic.AddInt64(&s.Count, count)
}

func (s *Statistics) AddSuccessSize(size int64) {
	atomic.AddInt64(&s.SuccessSize, size)
	atomic.AddInt64(&s.TotalSize, size)
}

func (s *Statistics) AddFailedCount(count int64) {
	atomic.AddInt64(&s.FailedCount, count)
	atomic.AddInt64(&s.Count, count)
}

func (s *Statistics) AddFailedSize(size int64) {
	atomic.AddInt64(&s.FailedSize, size)
	atomic.AddInt64(&s.TotalSize, size)
}

func (s *Statistics) Stat() string {
	totalSize := s.TotalSize
	totalCount := s.Count
	successSize := s.SuccessSize
	successCount := s.SuccessCount
	elapsed := time.Since(s.StartAt).Seconds()
	speed := float64(totalSize) / elapsed
	successSpeed := float64(successSize) / elapsed
	lastTotalCount := s.LastTotalCount
	lastTotalSize := s.LastTotalSize
	// lastSuccessCount := s.LastSuccessCount
	// lastSuccessSize := s.LastSuccessSize
	s.LastTotalCount = totalCount
	s.LastTotalSize = totalSize
	return fmt.Sprintf(
		`{"elapsed": %.2fs, "total_size": "%s", "total_count": %d, "speed": "%s/s", "success_size": "%s", "success_count": %d, "success_speed": "%s/s", "delta_count": %d, "delta_size": "%s"}`,
		elapsed,
		Size(totalSize).String(),
		totalCount,
		Size(speed).String(),
		Size(successSize).String(),
		successCount,
		Size(successSpeed).String(),
		totalCount-lastTotalCount,
		Size(totalSize-lastTotalSize).String(),
	)
}

func NewDownloader(args Args, options ...oss.Option) Downloader {
	var wg sync.WaitGroup
	if err := args.Validate(); err != nil {
		panic(err)
	}
	objectList := make(chan string, 1024)
	file, err := os.Open(args.InputFile)
	if err != nil {
		panic(err)
	}
	bucket, err := GetTestBucket(args)
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
	return Downloader{
		args:       args,
		bucket:     bucket,
		objectList: objectList,
		options:    options,
		wg:         &wg,
	}
}

func (d *Downloader) Start() {
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
func GetTestBucket(args Args) (*oss.Bucket, error) {
	// New client
	client, err := oss.New(args.Endpoint, args.AccessKey, args.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("connect to oss. error: %v", err)
	}

	// Get bucket
	bucket, err := client.Bucket(args.Bucket)
	if err != nil {
		return nil, fmt.Errorf("get bucket. error: %v", err)
	}

	return bucket, nil
}

// GetObjectSample shows the streaming download, range download and resumable download.
func (d *Downloader) GetObject(key string) (int64, error) {
	resp, err := d.bucket.GetObject(key, d.options...)
	if err != nil {
		return 0, err
	}
	defer resp.Close()
	return io.Copy(ioutil.Discard, resp)
}

func (d *Downloader) Stat() string {
	return d.stat.Stat()
}

func (s *Downloader) StatTicker(tick time.Duration) {
	ticker := time.NewTicker(tick)
	for range ticker.C {
		fmt.Println(s.Stat())
	}
}

func main() {
	flag.Parse()
	downloader := NewDownloader(args)
	go downloader.StatTicker(time.Duration(args.SampleInterval) * time.Millisecond)
	downloader.Start()
}
