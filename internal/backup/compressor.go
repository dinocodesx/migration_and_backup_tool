package backup

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Compressor provides a high-performance zstd compressed writer.
type Compressor struct {
	encoder *zstd.Encoder
}

// NewCompressor creates a new Compressor wrapping the provided writer.
// It uses the SpeedDefault level, which provides a good balance between CPU usage and compression ratio.
func NewCompressor(w io.Writer) (*Compressor, error) {
	encoder, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd writer: %w", err)
	}
	return &Compressor{encoder: encoder}, nil
}

// Write writes data through the compressor to the underlying writer.
// The data is buffered internally and compressed according to the zstd algorithm.
func (c *Compressor) Write(p []byte) (n int, err error) {
	return c.encoder.Write(p)
}

// Flush flushes the encoder's buffers to the underlying writer without closing the stream.
// Useful for ensuring data is sent during long-running streaming operations.
func (c *Compressor) Flush() error {
	return c.encoder.Flush()
}

// Close finalizes the compression block and closes the encoder.
// It MUST be called to write the zstd footer and ensure all data is flushed.
func (c *Compressor) Close() error {
	return c.encoder.Close()
}
