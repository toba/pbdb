// Package index reverses the normal key-value relationship of stored items
// so those items can be looked up by their values.
package index

import (
	"bytes"

	"toba.io/lib/oops"

	"github.com/boltdb/bolt"
)

type (
	// baseIndex wraps a Bolt bucket used to store item values mapped back to
	// their item. It is the basis for the other index types.
	baseIndex struct{ Bucket *bolt.Bucket }

	// mapper function returns either the key or value bytes of an index.
	mapper func(k, v []byte) []byte
)

// keySeparator is used between the value key and item key to create a unique
// composite key for non-unique indexes.
//
// Example:
//		key1<0xFF>value1 -> value1
//		key1<0xFF>value2 -> value2
//
const keySeparator = 0xFF

// add creates a new bucket entry for a value and item pair or returns an error
// if the same value is already indexed to a different item.
//
// For unique indexes, the valueKey is the direct byte conversion of the item
// value being indexed. For non-unique indexes, the valueKey is a composite of
// the value and item key, with a separator, in order to be unique within the
// Bolt bucket.
func (idx *baseIndex) add(valueKey, itemKey []byte) error {
	if err := validKeys(valueKey, itemKey); err != nil {
		return err
	}

	maybe := idx.Bucket.Get(valueKey)

	if maybe != nil {
		if bytes.Equal(maybe, itemKey) {
			// same value is already indexed to this item key
			return nil
		}
		// same value is aleady indexed to a different item key
		return oops.AlreadyExists
	}
	return idx.Bucket.Put(valueKey, itemKey)
}

// keysWithItem returns all bucket keys for which the item is the value.
func (idx *baseIndex) keysWithItem(itemKey []byte) [][]byte {
	c := idx.Bucket.Cursor()
	var keys [][]byte

	for k, v := c.First(); v != nil; k, v = c.Next() {
		if bytes.Equal(v, itemKey) {
			keys = append(keys, k)
		}
	}
	if len(keys) > 0 {
		return keys
	}
	return nil
}

// keysWithPrefix finds all keys prefixed by a value key plus seperator.
func (idx *baseIndex) keysWithPrefix(keyPrefix []byte) [][]byte {
	return idx.allWithPrefix(keyPrefix, keyMap)
}

// itemsWithValuePrefix returns all items for keys having a prefix.
func (idx *baseIndex) itemsWithValuePrefix(keyPrefix []byte) [][]byte {
	return idx.allWithPrefix(keyPrefix, valueMap)
}

// removeItem finds all bucket keys for which the item is a the value and
// deletes them. There is no error if a match is not found.
func (idx *baseIndex) removeItem(itemKey []byte) error {
	keys := idx.keysWithItem(itemKey)
	if keys == nil {
		return nil
	}

	for _, k := range keys {
		err := idx.Bucket.Delete(k)
		if err != nil {
			return err
		}
	}
	return nil
}

// forRange execute a function for each key-value in a range of key values.
func (idx *baseIndex) forRange(min, max []byte, fn func(k, v []byte) error) error {
	c := idx.Bucket.Cursor()

	for k, v := c.Seek(min); v != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// allInRange returns a list of key or value bytes, using a mapper function,
// for a range of key values.
func (idx *baseIndex) allInRange(min, max []byte, m mapper) ([][]byte, error) {
	var list [][]byte

	idx.forRange(min, max, func(k, v []byte) error {
		list = append(list, m(k, v))
		return nil
	})

	return list, nil
}

// allWithPrefix returns a list of key or value bytes, using a mapper function,
// for all keys with a given prefix.
func (idx *baseIndex) allWithPrefix(valueKey []byte, m mapper) [][]byte {
	c := idx.Bucket.Cursor()
	prefix := makePrefix(valueKey)
	var list [][]byte

	for k, v := c.Seek(firstPrefix(valueKey)); bytes.HasPrefix(k, prefix); k, v = c.Next() {
		list = append(list, m(k, v))
	}

	if len(list) > 0 {
		return list
	}
	return nil
}
