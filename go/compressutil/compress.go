package compressutil

import (
	"bytes"
	sflate "compress/flate"
	zb "compress/zlib"
	"encoding/hex"
	"io"

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

func zlib_compress(str string) (string, error) {
	// 创建字节数组输出缓冲区
	outputBuffer := new(bytes.Buffer)
	// 创建压缩写入器
	zlibWriter, err := zb.NewWriterLevel(outputBuffer, zb.DefaultCompression)
	if err != nil {
		return "", err
	}
	// 将输入字节数组写入压缩写入器
	_, err = zlibWriter.Write([]byte(str))
	if err != nil {
		return "", err
	}
	// 关闭压缩写入器
	err = zlibWriter.Close()
	if err != nil {
		return "", err
	}
	// 获取压缩后的字节数组
	compressedBytes := outputBuffer.Bytes()
	// 将压缩后的字节数组转换为字符串
	return hex.EncodeToString(compressedBytes), nil
}

func zlib_uncompress(str []byte) ([]byte, error) {
	// 将压缩后的字符串转换为字节数组
	compressedBytes, _ := hex.DecodeString(string(str))

	// 创建字节数组输入缓冲区
	inputBuffer := bytes.NewBuffer(compressedBytes)

	// 创建解压缩读取器
	zlibReader, err := zb.NewReader(inputBuffer)
	if err != nil {
		return nil, err
	}

	// 读取解压缩数据
	decompressedBytes, err := io.ReadAll(zlibReader)
	if err != nil {
		return nil, err
	}

	// 关闭解压缩读取器
	zlibReader.Close()

	// 将解压缩后的字节数组转换为字符串
	return decompressedBytes, nil
}

// UnCompressData is used to decompress the data.
func UnCompressData(val []byte) ([]byte, error) {
	if len(val) > 0 && val[0] == '{' {
		return val, nil
	}
	return zlib_uncompress(val)
}
