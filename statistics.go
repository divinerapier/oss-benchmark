package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

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
