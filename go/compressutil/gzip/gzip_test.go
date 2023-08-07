package gzip

import (
	"testing"

	"compress/gzip"
)

const (
	val = `{"7fresh_kc_admin": {"Password": "uNl7gsTMp+VoHyQ2BMYDrFAz1jCB+4mx+rfGOH3MtwE=","UserData": "","KeySpaces":[{ "Name": "7fresh_kc" }]},"7fresh_kc_ro": {"Password": "uNl7gsTMp+VoHyQ2BMYDrFAz1jCB+4mx+rfGOH3MtwE=","UserData": "", "KeySpaces": [{"Name": "7fresh_kc"}]},"7fresh_kc_rr": {"Password": "uNl7gsTMp+VoHyQ2BMYDrFAz1jCB+4mx+rfGOH3MtwE=","UserData": "","KeySpaces": [{"Name": "7fresh_kc"}]}}`
)

func TestEncode(t *testing.T) {
	c := NewGzipCompress(gzip.BestCompression)
	data, count, err := c.Encode([]byte(val))
	if err != nil {
		t.Fatalf("%v", err)
	}

	t.Logf("oldLen:%v newLen:%v count:%v", len(val), len(data), count)
}

func TestDecode(t *testing.T) {
	c := NewGzipCompress(gzip.BestCompression)
	data, _, err := c.Encode([]byte(val))
	if err != nil {
		t.Fatalf("%v", err)
	}

	out, err := c.Decode(data)
	if err != nil {
		t.Fatalf("%v", err)
	}

	t.Logf("%v", string(out))
}
