package storage

import (
	"bytes"
	"compress/gzip"
	"github.com/klauspost/compress/zstd"
	"io"
	"sync"
)

// zstdEncoder is shared by every CompressData(..., CompressionZstd) call in
// the process -- i.e. it's the encoder actually used for the database's
// document-blob compression (see ops_insert.go/ops_local.go/transaction.go).
// It's tuned for lower resource consumption rather than maximum ratio:
//   - WithEncoderConcurrency(1): the default is GOMAXPROCS, which allocates
//     that many independent encoder match-finder states up front. Document
//     blobs here are one-shot EncodeAll calls on already-small (post
//     value-threshold) payloads, not a single huge stream that benefits
//     from being split across cores, so that multi-core state is pure
//     overhead -- pinning it to 1 encoder instance is the single biggest
//     memory win available here.
//   - WithLowerEncoderMem(true): trims additional internal buffers at a
//     small speed cost, on top of the concurrency reduction above.
//   - SpeedFastest: the lowest compression level, which also uses the
//     smallest match-finder tables (less memory) and is the cheapest CPU
//     mode -- appropriate since CompressData already only engages for
//     payloads >= 1KB and only keeps the result when it beats a 90%
//     ratio, so this never needs to fight for the last few percent of
//     ratio to be worthwhile.
var zstdEncoder, _ = zstd.NewWriter(nil,
	zstd.WithEncoderLevel(zstd.SpeedFastest),
	zstd.WithEncoderConcurrency(1),
	zstd.WithLowerEncoderMem(true),
)

// zstdDecoder mirrors the same lower-memory posture on the read side.
var zstdDecoder, _ = zstd.NewReader(nil,
	zstd.WithDecoderConcurrency(1),
	zstd.WithDecoderLowmem(true),
)

var gzipPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}

type CompressionType string

const (
	CompressionNone CompressionType = "none"
	CompressionZstd CompressionType = "zstd"
	CompressionGzip CompressionType = "gzip"
)

func CompressData(data []byte, ctype CompressionType) ([]byte, bool, error) {
	if len(data) < 1024 {
		return data, false, nil
	}

	switch ctype {
	case CompressionZstd:
		compressed := zstdEncoder.EncodeAll(data, nil)
		if len(compressed) < len(data)*90/100 {
			return compressed, true, nil
		}
	case CompressionGzip:
		var buf bytes.Buffer
		gz := gzipPool.Get().(*gzip.Writer)
		gz.Reset(&buf)
		if _, err := gz.Write(data); err != nil {
			gz.Close()
			return data, false, err
		}
		gz.Close()
		gzipPool.Put(gz)
		compressed := buf.Bytes()
		if len(compressed) < len(data)*90/100 {
			return compressed, true, nil
		}
	}
	return data, false, nil
}

func DecompressData(data []byte) ([]byte, error) {
	if decompressed, err := zstdDecoder.DecodeAll(data, nil); err == nil {
		return decompressed, nil
	}
	if r, err := gzip.NewReader(bytes.NewReader(data)); err == nil {
		defer r.Close()
		return io.ReadAll(r)
	}
	return data, nil
}
