package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

var ErrTornRecord = errors.New("storage: torn record")

// encodeRecord frames payload as [length][payload][crc32] ready to append.
func encodeRecord(payload []byte) []byte {
	buf := make([]byte, 4+len(payload)+4)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	copy(buf[4:4+len(payload)], payload)
	checksum := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(buf[4+len(payload):], checksum)
	return buf
}

func decodeRecord(r io.Reader) ([]byte, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, ErrTornRecord
	}
	length := binary.BigEndian.Uint32(lengthBuf)

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, ErrTornRecord
	}

	checksumBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, checksumBuf); err != nil {
		return nil, ErrTornRecord
	}
	checksum := binary.BigEndian.Uint32(checksumBuf)

	if crc32.ChecksumIEEE(payload) != checksum {
		return nil, ErrTornRecord
	}

	return payload, nil
}
