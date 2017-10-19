package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"toba.io/lib/oops"
)

// Encode converts a BoltValue to a gob byte array.
func Encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	en := gob.NewEncoder(&buf)
	err := en.Encode(value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode converts Bolt stored gob value to a BoltValue interface.
func Decode(data []byte, value interface{}) error {
	var buf bytes.Buffer
	decoder := gob.NewDecoder(&buf)

	_, err := buf.Write(data)
	if err != nil {
		return err
	}
	return decoder.Decode(value)
}

// NumberToBytes converts numbers into a byte slice for storage. The format is big endian
// so that a sort of the slices matches the numeric order.
func NumberToBytes(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	var raw interface{}

	switch t := v.(type) {
	case int:
		raw = int64(t)
	case uint:
		raw = uint64(t)
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		raw = v
	default:
		return nil, oops.NotNumeric
	}

	if raw == nil {
		return nil, nil
	}

	err := binary.Write(&buf, binary.BigEndian, raw)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// NumberFromBytes convers a byte slice to a number.
func NumberFromBytes(raw []byte) (int64, error) {
	r := bytes.NewReader(raw)
	var to int64
	err := binary.Read(r, binary.BigEndian, &to)
	if err != nil {
		return 0, err
	}
	return to, nil
}
