package storage

import (
	"gopkg.in/cheggaaa/pb.v1"
	"time"
	"log"
)

type progressBar interface {
	Add(count int) int
	Finish()
}

func newConsoleProgressBar(count int) progressBar {
	bar := pb.StartNew(count)
	bar.SetRefreshRate(time.Second)
	return bar
}

type logProgressBar struct {
	total      int
	count      int
	lastLogged int
}

func (b *logProgressBar) Add(count int) int {
	b.count += count
	if b.total > 0 {
		percentage := b.count * 100 / b.total
		if percentage > b.lastLogged {
			log.Printf("Progress: %d (%d/%d)", percentage, b.count, b.total)
			b.lastLogged = percentage
		}
	}
	return b.count
}

func (b *logProgressBar) Finish() {
	// TODO
}

func newLogProgressBar(count int) progressBar {
	return &logProgressBar{total: count}
}

type Statistics struct {
	fileCount  int
	filesFound int
	hashTot    int

	showPb   bool
	progress progressBar
	passName string
	start    time.Time
}

func NewProgressBarStats() *Statistics {
	return &Statistics{showPb: true}
}

func NewProgressLogStats() *Statistics {
	return &Statistics{showPb: false}
}

func (s *Statistics) startProgress(name string, count int) {
	s.passName = name
	s.start = time.Now()
	s.progress = newLogProgressBar(count)
	if s.showPb {
		s.progress = newConsoleProgressBar(count)
	}
}

func (s *Statistics) updateProgress(count int) {
	s.progress.Add(count)
}

func (s *Statistics) StopProgress() {
	duration := time.Since(s.start)
	s.progress.Finish()
	log.Printf("Pass %s completed in %s", s.passName, duration)
}

func (s *Statistics) SetFileCount(count int) {
	s.fileCount = count
}

func (s *Statistics) StartFileinfoProgress() {
	s.startProgress("Collecting file information", s.fileCount)
}

func (s *Statistics) FileInfoRead() {
	s.updateProgress(1)
}

func (s *Statistics) FileAdded() {
	s.filesFound += 1
}

func (s *Statistics) HashesCalculated(count int) {
	s.hashTot += count
	s.updateProgress(count)
}

func (s *Statistics) Deduplicating(count int) {
	s.updateProgress(count)
}

func (s *Statistics) StartHashProgress() {
	s.startProgress("Calculating hashes for first block of each file", s.filesFound)
}

func (s *Statistics) StartDedupProgress() {
	s.startProgress("Deduplication", s.hashTot)
}
