package backup

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Compressor provides a high-performance stream compressor using the Zstandard algorithm.
// It wraps a raw io.Writer and compresses data written to it on the fly.
type Compressor struct {
	// encoder is the underlying zstd stream encoder.
	encoder *zstd.Encoder
}

// NewCompressor initializes a new Compressor that writes compressed data to 'w'.
// It uses the SpeedDefault level, balancing throughput and compression ratio.
func NewCompressor(w io.Writer) (*Compressor, error) {
	encoder, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd writer: %w", err)
	}
	return &Compressor{encoder: encoder}, nil
}

// Write compresses the provided byte slice and writes it to the underlying stream.
func (c *Compressor) Write(p []byte) (n int, err error) {
	return c.encoder.Write(p)
}

// Flush forces any buffered data to be compressed and written to the underlying stream.
// It is useful for long-running processes to ensure data is periodically persisted.
func (c *Compressor) Flush() error {
	return c.encoder.Flush()
}

// Close finalizes the compression block and releases resources. It must be called
// to ensure the stream is valid and all data is written.
func (c *Compressor) Close() error {
	return c.encoder.Close()
}
