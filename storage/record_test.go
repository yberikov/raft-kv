package storage

import (
	"bytes"
	"io"
	"testing"
)

func TestRecordRoundTrip(t *testing.T) {
	cases := [][]byte{
		[]byte("hello"),
		[]byte(""),
		bytes.Repeat([]byte{0xAB}, 1000),
	}
	for _, payload := range cases {
		frame := encodeRecord(payload)
		got, err := decodeRecord(bytes.NewReader(frame))
		if err != nil {
			t.Fatalf("decodeRecord: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("got %q, want %q", got, payload)
		}
	}
}

func TestRecordMultipleInOneStream(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(encodeRecord([]byte("first")))
	buf.Write(encodeRecord([]byte("second")))

	r := bytes.NewReader(buf.Bytes())

	first, err := decodeRecord(r)
	if err != nil || string(first) != "first" {
		t.Fatalf("first record: got %q, err %v", first, err)
	}
	second, err := decodeRecord(r)
	if err != nil || string(second) != "second" {
		t.Fatalf("second record: got %q, err %v", second, err)
	}
	if _, err := decodeRecord(r); err != io.EOF {
		t.Fatalf("expected io.EOF at clean end of stream, got %v", err)
	}
}

func TestRecordTornAtLengthPrefix(t *testing.T) {
	frame := encodeRecord([]byte("hello"))
	truncated := frame[:2] // cut mid length-prefix
	if _, err := decodeRecord(bytes.NewReader(truncated)); err != ErrTornRecord {
		t.Fatalf("got %v, want ErrTornRecord", err)
	}
}

func TestRecordTornAtPayload(t *testing.T) {
	frame := encodeRecord([]byte("hello world"))
	truncated := frame[:4+3] // full length prefix, only part of the payload
	if _, err := decodeRecord(bytes.NewReader(truncated)); err != ErrTornRecord {
		t.Fatalf("got %v, want ErrTornRecord", err)
	}
}

func TestRecordTornAtChecksum(t *testing.T) {
	frame := encodeRecord([]byte("hello"))
	truncated := frame[:len(frame)-2] // missing part of the checksum
	if _, err := decodeRecord(bytes.NewReader(truncated)); err != ErrTornRecord {
		t.Fatalf("got %v, want ErrTornRecord", err)
	}
}

func TestRecordCorruptedPayload(t *testing.T) {
	frame := encodeRecord([]byte("hello"))
	frame[4] ^= 0xFF // flip a bit inside the payload, checksum now stale
	if _, err := decodeRecord(bytes.NewReader(frame)); err != ErrTornRecord {
		t.Fatalf("got %v, want ErrTornRecord", err)
	}
}
