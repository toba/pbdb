// Package key generates and manages ULID database keys.
package key

import (
	"bytes"
	"crypto/rand"

	"github.com/oklog/ulid"
)

var (
	// Zero is the minimum standard key value
	Zero = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	// Max is the maximum standard key value
	Max = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

// Create generates a lexicographically sortable byte array. ULID is not used
// directly because BoltDB uses byte slices instead of byte arrays.
// See https://github.com/oklog/ulid
func Create() ([]byte, error) {
	//entropy := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	entropy := rand.Reader
	key, err := ulid.New(ulid.Now(), entropy)

	if err != nil {
		return nil, err
	}
	return key[:], nil
}

// ToString converts an index key to its string representation. If the key
// is not a ULID or primitive type then the string may contain non-printing
// characters.
func ToString(key []byte) string {
	if key == nil {
		return ""
	}
	if IsValid(key) {
		var fix [16]byte
		copy(fix[:], key)
		id := ulid.ULID(fix)
		return id.String()
	}
	return string(key)

}

// FromString parses a string as a ULID byte array or does a simple
// type conversion if that fails.
func FromString(raw string) []byte {
	if raw == "" {
		return nil
	}
	id, err := ulid.Parse(raw)
	if err == nil {
		return id[:]
	}
	return []byte(raw)
}

// IsValid tests whether a byte array is the right length to be a ULID.
func IsValid(raw []byte) bool {
	return raw != nil && len(raw) == 16
}

// IsEmpty tests whether a byte array is empty.
func IsEmpty(raw []byte) bool {
	return raw == nil || len(raw) == 0
}

// ListContains tests whether a key is contained in a list of keys.
func ListContains(list [][]byte, key []byte) bool {
	if list == nil || key == nil || len(list) == 0 || len(key) == 0 {
		return false
	}
	for _, k := range list {
		if bytes.Equal(k, key) {
			return true
		}
	}
	return false
}

// MergeLists combines key lists into a single list of unique values.
//
// Iteration is required because Go does not allow byte slices as hash map keys.
func MergeLists(lists ...[][]byte) [][]byte {
	var merged [][]byte

	for _, list := range lists {
		if list == nil {
			continue
		}
		for _, k := range list {
			if ListContains(merged, k) {
				continue
			}
			merged = append(merged, k)
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}
