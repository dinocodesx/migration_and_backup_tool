package backup

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Compressor provides a zstd compressed writer.
type Compressor struct {
	encoder *zstd.Encoder
}

// NewCompressor creates a new Compressor wrapping the provided writer.
func NewCompressor(w io.Writer) (*Compressor, error) {
	encoder, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd writer: %w", err)
	}
	return &Compressor{encoder: encoder}, nil
}

// Write writes compressed data to the underlying writer.
func (c *Compressor) Write(p []byte) (n int, err error) {
	return c.encoder.Write(p)
}

// Flush flushes the encoder's buffers.
func (c *Compressor) Flush() error {
	return c.encoder.Flush()
}

// Close finalizes the compression and closes the encoder.
func (c *Compressor) Close() error {
	return c.encoder.Close()
}
