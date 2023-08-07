package zlib

import (
	"bytes"
	"compress/zlib"
	"io"
)

// Compress represent the zlib object.
type Compress struct {
	level int
}

// NewZlibCompress - create new zlib.
func NewZlibCompress(level int) *Compress {
	return &Compress{level: level}
}

// Encode - encode.
func (g *Compress) Encode(data []byte) ([]byte, int, error) {
	var b bytes.Buffer
	var w *zlib.Writer
	var err error

	w, err = zlib.NewWriterLevel(&b, g.level)
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

// Decode - uncompress data.
func (g *Compress) Decode(data []byte) ([]byte, error) {
	b := bytes.NewBuffer(data)

	r, err := zlib.NewReader(b)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(r)
}
