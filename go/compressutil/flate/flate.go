package flate

import (
	"bytes"
	"compress/flate"
	"io"
)

// Compress represent the flate object.
type Compress struct {
	level int
}

// NewFlateCompress - create new flate.
func NewFlateCompress(level int) *Compress {
	return &Compress{level: level}
}

// Encode - encode.
func (g *Compress) Encode(data []byte) ([]byte, int, error) {
	var b bytes.Buffer
	var w *flate.Writer
	var err error

	w, err = flate.NewWriter(&b, g.level)
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
func (g *Compress) Decode(val []byte) ([]byte, error) {

	b := bytes.NewBuffer(val)

	r := flate.NewReader(b)
	defer r.Close()
	return io.ReadAll(r)
}
