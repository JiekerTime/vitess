package compressutil

import (
	sflate "compress/flate"
	"encoding/hex"

	"vitess.io/vitess/go/compressutil/flate"
	"vitess.io/vitess/go/compressutil/gzip"
	"vitess.io/vitess/go/compressutil/zlib"
)

const (
	compressFlate = iota
	compressGzip
	compressZlib

	defaultLevel        = sflate.BestCompression
	defaultCompressType = compressFlate
)

// Compress - define compress interface.
type Compress interface {
	Encode([]byte) ([]byte, int, error)
	Decode([]byte) ([]byte, error)
}

// NewCompress make a new compress object.
func NewCompress(t int) Compress {
	if t == compressFlate {
		return flate.NewFlateCompress(defaultLevel)
	}

	if t == compressGzip {
		return gzip.NewGzipCompress(defaultLevel)
	}

	if t == compressZlib {
		return zlib.NewZlibCompress(defaultLevel)
	}

	return nil
}

// CompressData - compress the data and return compressed data.
func CompressData(val []byte) ([]byte, error) {
	c := NewCompress(defaultCompressType)

	data, _, err := c.Encode(val)
	if err != nil {
		return nil, err
	}

	return []byte(hex.EncodeToString(data)), nil
}

// UnCompressData is used to decompress the data.
func UnCompressData(val []byte) ([]byte, error) {
	if len(val) > 0 && val[0] == '{' {
		return val, nil
	}

	data, err := hex.DecodeString(string(val))
	if err != nil {
		return nil, err
	}

	c := NewCompress(defaultCompressType)
	out, err := c.Decode(data)
	if err != nil {
		return nil, err
	}

	return out, nil
}
