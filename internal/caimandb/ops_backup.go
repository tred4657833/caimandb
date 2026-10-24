package caimandb

import (
	"bufio"
	"compress/gzip"
	"go.uber.org/zap"
	"io"
	"os"
	"strings"
	"time"
)

func (e *Engine) Backup(dbName, destFile string) error {
	start := time.Now()

	if !e.dirMgr.DBExists(dbName) {
		return ErrDBNotFound
	}

	db, err := e.pool.OpenDataPath(e.dirMgr.DBPath(dbName))
	if err != nil {
		return err
	}

	f, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 128<<20)
	_, err = db.Backup(bw, 0)
	if err != nil {
		return err
	}
	err = bw.Flush()

	if strings.HasSuffix(destFile, ".gz") {
		f.Close()
		data, err := os.ReadFile(destFile)
		if err != nil {
			return err
		}
		gzFile, err := os.Create(destFile)
		if err != nil {
			return err
		}
		defer gzFile.Close()
		gz := gzip.NewWriter(gzFile)
		defer gz.Close()
		if _, err := gz.Write(data); err != nil {
			return err
		}
	}

	log().Info("Backup completed",
		zap.String("db", dbName),
		zap.String("dest", destFile),
		zap.Duration("duration", time.Since(start)))

	return err
}

func (e *Engine) Restore(dbName, srcFile string) error {
	start := time.Now()

	if !e.dirMgr.DBExists(dbName) {
		if err := e.CreateDB(dbName); err != nil {
			return err
		}
	}

	var reader io.Reader

	f, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer f.Close()

	if strings.HasSuffix(srcFile, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gz.Close()
		reader = gz
	} else {
		reader = f
	}

	db, err := e.pool.OpenDataPath(e.dirMgr.DBPath(dbName))
	if err != nil {
		return err
	}

	err = db.Load(bufio.NewReaderSize(reader, 128<<20), 512)

	log().Info("Restore completed",
		zap.String("db", dbName),
		zap.String("src", srcFile),
		zap.Duration("duration", time.Since(start)))

	return err
}
