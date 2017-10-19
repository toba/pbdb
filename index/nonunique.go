package index

import (
	"bytes"

	"toba.io/lib/db/key"
	"toba.io/lib/oops"
)

// NonUnique is an index that matches non-unique item values to their item key,
// allowing multiple items to have the same field value.
//
// The structure is one nested bucket per value:
//    value1_item1 -> item1
//    value2_item1 -> item1
//    value2_item3 -> item3
//    ...
type NonUnique struct{ baseIndex }

// Add a value indexed to its item key. The bucket key is a composite of the value
// and item keys to allow multiple values per item.
//
// The composite key will lead to erroneous matches if it plus the separator has
// the same initial bytes as a longer value key.
func (idx *NonUnique) Add(valueKey, itemKey []byte) error {
	if err := validKeys(valueKey, itemKey); err != nil {
		return err
	}
	return idx.add(makeCompositeKey(valueKey, itemKey), itemKey)
}

// RemoveValue deletes all bucket items with a key prefixed by a value.
func (idx *NonUnique) RemoveValue(valueKey []byte) error {
	if key.IsEmpty(valueKey) {
		return oops.InvalidIndexKey
	}
	keys := idx.keysWithPrefix(valueKey)

	if keys == nil {
		return nil
	}

	for _, k := range keys {
		if err := idx.Bucket.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// RemoveItem removes an item key from all entries of which it was part.
func (idx *NonUnique) RemoveItem(itemKey []byte) error {
	return idx.removeItem(itemKey)
}

// FirstWithValue returns the first item key indexed to a value.
func (idx *NonUnique) FirstWithValue(valueKey []byte) []byte {
	c := idx.Bucket.Cursor()
	k, itemKey := c.Seek(firstPrefix(valueKey))
	// if seek does not find a match it stops at the next key
	// (the first one lexically higher than the valueKey)
	if bytes.HasPrefix(k, makePrefix(valueKey)) {
		return itemKey
	}
	return nil
}

// AllWithValue returns all item keys indexed to a value.
func (idx *NonUnique) AllWithValue(valueKey []byte, opts *QueryOptions) ([][]byte, error) {
	return idx.itemsWithValuePrefix(valueKey), nil
}

// All returns all unique item keys in the index.
func (idx *NonUnique) All(opts *QueryOptions) ([][]byte, error) {
	items, err := allValues(idx.Bucket)
	if err != nil {
		return nil, err
	}
	return unique(items), nil
}

// AllInRange returns the unique item keys corresponding to a range of values.
func (idx *NonUnique) AllInRange(min, max []byte, opts *QueryOptions) ([][]byte, error) {
	items, err := idx.allInRange(firstPrefix(min), lastPrefix(max), valueMap)
	if err != nil {
		return nil, err
	}
	return unique(items), nil
}
