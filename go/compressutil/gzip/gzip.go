package gzip

import (
	"bytes"
	"compress/gzip"
	"io"
)

// Compress represent the gzip object.
type Compress struct {
	level int
}

// NewGzipCompress - create new gzip.
func NewGzipCompress(level int) *Compress {
	return &Compress{level: level}
}

// Encode - encode
func (g *Compress) Encode(data []byte) ([]byte, int, error) {
	var b bytes.Buffer
	var w *gzip.Writer
	var err error

	w, err = gzip.NewWriterLevel(&b, g.level)
	if err != nil {
		return nil, 0, err
	}

	count, err := w.Write(data)
	if err != nil {
		return nil, 0, err
	}

	_ = w.Close()

	return b.Bytes(), count, nil
}

// Decode - decompress data.
func (g *Compress) Decode(data []byte) ([]byte, error) {
	b := bytes.NewBuffer(data)

	r, err := gzip.NewReader(b)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(r)
}
