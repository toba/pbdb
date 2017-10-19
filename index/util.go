package index

import (
	"github.com/boltdb/bolt"
	"github.com/toba/pbdb/key"
	"toba.io/lib/oops"
)

// allInBucket transforms all bucket items to a list of byte slices using a
// mapping function.
func allInBucket(bucket *bolt.Bucket, m mapper) ([][]byte, error) {
	var list [][]byte

	if bucket == nil {
		return nil, nil
	}

	bucket.ForEach(func(k, v []byte) error {
		list = append(list, m(k, v))
		return nil
	})

	return list, nil
}

// valueMap returns a Bolt item value. For indexes, this is the item key.
func valueMap(k, v []byte) []byte {
	return v
}

// keyMap returns a Bolt item key. For indexes, this is the value that was
// indexed.
func keyMap(k, v []byte) []byte {
	return k
}

// allValues returns all values (item keys in the case of an index) from a
// bucket.
func allValues(bucket *bolt.Bucket) ([][]byte, error) {
	return allInBucket(bucket, valueMap)
}

// allKeys returns all keys from a bucket.
func allKeys(bucket *bolt.Bucket) ([][]byte, error) {
	return allInBucket(bucket, keyMap)
}

// validKeys evaluates both the value and item keys for proper length.
func validKeys(valueKey, itemKey []byte) error {
	if key.IsEmpty(valueKey) {
		return oops.InvalidIndexKey
	}
	if !key.IsValid(itemKey) {
		return oops.InvalidItemKey
	}
	return nil
}

// firstPrefix is used to seek the first key matching a prefix.
func firstPrefix(valueKey []byte) []byte {
	return append(makePrefix(valueKey), key.Zero...)
}

// lastPrefix is used to limit a prefix range query.
func lastPrefix(valueKey []byte) []byte {
	return append(makePrefix(valueKey), key.Max...)
}

// makePrefix adds the bytes to a key used to designate it as a prefix.
func makePrefix(valueKey []byte) []byte {
	return append(valueKey, keySeparator)
}

// makeCompositKey builds a key combined with its value, used for
// non-unique indexes.
func makeCompositeKey(valueKey, itemKey []byte) []byte {
	return append(makePrefix(valueKey), itemKey...)
}

// unique updates a list so it contains only unique keys.
func unique(keys [][]byte) [][]byte {
	if keys == nil {
		return keys
	}
	return key.MergeLists(make([][]byte, 0), keys)
}
