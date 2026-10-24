package caimandb

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type AuditLog struct {
	Timestamp time.Time `json:"timestamp"`
	UserID    string    `json:"user_id"`
	Username  string    `json:"username"`
	Action    string    `json:"action"`
	DB        string    `json:"db,omitempty"`
	Block     string    `json:"block,omitempty"`
	DocID     string    `json:"doc_id,omitempty"`
	ShardID   string    `json:"shard_id,omitempty"`
	Duration  int64     `json:"duration_ms,omitempty"`
	Status    string    `json:"status"`
	Message   string    `json:"message,omitempty"`
	IP        string    `json:"ip,omitempty"`
	UserAgent string    `json:"user_agent,omitempty"`
}

type AuditLogger struct {
	mu       sync.Mutex
	file     *os.File
	encoder  *json.Encoder
	buf      *bufio.Writer
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewAuditLogger(path string) (*AuditLogger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}

	al := &AuditLogger{
		file:     f,
		buf:      bufio.NewWriterSize(f, 128<<20),
		stopChan: make(chan struct{}),
	}
	al.encoder = json.NewEncoder(al.buf)

	al.wg.Add(1)
	safeGo("audit_flush_loop", al.flushLoop)

	return al, nil
}

func (al *AuditLogger) flushLoop() {
	defer al.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			al.mu.Lock()
			al.buf.Flush()
			al.mu.Unlock()
		case <-al.stopChan:
			al.mu.Lock()
			al.buf.Flush()
			al.file.Sync()
			al.mu.Unlock()
			return
		}
	}
}

func (al *AuditLogger) Log(entry AuditLog) {
	entry.Timestamp = time.Now()
	al.mu.Lock()
	defer al.mu.Unlock()
	al.encoder.Encode(entry)
}

func (al *AuditLogger) Close() error {
	close(al.stopChan)
	al.wg.Wait()
	return al.file.Close()
}
