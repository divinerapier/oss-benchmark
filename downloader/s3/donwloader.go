package s3

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/divinerapier/oss-benchmark/args"
	"github.com/divinerapier/oss-benchmark/stat"
	"github.com/divinerapier/oss-benchmark/writer"
	"github.com/golang/glog"
)

const (
	Aws  = "aws-s3"
	Ceph = "ceph-s3"
)

type Session struct {
	cfg        *args.Args
	session    *session.Session
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
}

func NewSession(arg args.Args) *Session {
	var awsConfig *aws.Config
	cred := credentials.NewStaticCredentials(
		arg.AccessKey,
		arg.SecretKey, "")
	switch arg.Provider {
	case Aws:
		awsConfig = aws.NewConfig().
			WithRegion(arg.Region).
			WithDisableSSL(false).
			WithCredentials(cred)

	case Ceph:
		awsConfig = aws.NewConfig().
			WithRegion("ceph").
			WithDisableSSL(true).
			WithEndpoint(arg.Endpoint).
			WithS3ForcePathStyle(true).
			WithCredentials(cred)
	default:
		panic("unknown s3 provider")
	}
	awsSession := session.Must(session.NewSession(awsConfig))
	s := Session{
		cfg:        &arg,
		session:    awsSession,
		downloader: s3manager.NewDownloader(awsSession),
		uploader:   s3manager.NewUploader(awsSession),
	}
	s.downloader.Concurrency = arg.Threads
	s.uploader.Concurrency = arg.Threads

	return &s
}

type Downloader struct {
	session    *Session
	objectList chan string
	stat       stat.Statistics
	wg         *sync.WaitGroup
}

// GetObjectSample shows the streaming download, range download and resumable download.
func (d *Downloader) GetObject(key string) (int64, error) {
	return d.session.downloader.Download(&writer.DiscardWriterAt{}, &s3.GetObjectInput{
		Bucket: aws.String(d.session.cfg.Bucket),
		Key:    aws.String(key),
	})
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

func NewDownloader(arg args.Args) *Downloader {
	var wg sync.WaitGroup
	if err := arg.Validate(); err != nil {
		panic(err)
	}
	objectList := make(chan string, 1024)
	file, err := os.Open(arg.InputFile)
	if err != nil {
		panic(err)
	}

	session := NewSession(arg)
	downloader := &Downloader{
		session:    session,
		objectList: objectList,
		wg:         &wg,
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

func (d *Downloader) Start() {
	d.stat.Start()
	defer d.wg.Wait()
	var wg sync.WaitGroup
	for i := 0; i < d.session.cfg.Threads; i++ {
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
