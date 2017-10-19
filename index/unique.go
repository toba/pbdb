package index

// Unique is an index of non-repeating values stored in their own bucket
// that reference standard item keys in a separate bucket.
//
// The structure is a simple mapping between values and their item key:
//    value1 -> item1
//    value2 -> item2
//    ...
type Unique struct{ baseIndex }

// Add a value and its target item key to the index.
func (idx *Unique) Add(valueKey, itemKey []byte) error {
	return idx.add(valueKey, itemKey)
}

// RemoveItem removes an item key from the unique index by iterating over all
// bucket contents until itemKey is found.
func (idx *Unique) RemoveItem(itemKey []byte) error {
	return idx.removeItem(itemKey)
}

// RemoveValue removes a value key from the index.
func (idx *Unique) RemoveValue(valueKey []byte) error {
	return idx.Bucket.Delete(valueKey)
}

// FirstWithValue returns the first item key matched to an indexed value. For
// a unique index, this is the only item matched to a value.
func (idx *Unique) FirstWithValue(valueKey []byte) []byte {
	return idx.Bucket.Get(valueKey)
}

// AllWithValue returns the item keys referenced by a value key. For
// a unique index, this will always be zero or one items.
func (idx *Unique) AllWithValue(valueKey []byte, opts *QueryOptions) ([][]byte, error) {
	return [][]byte{idx.Bucket.Get(valueKey)}, nil
}

// All returns all item keys in the index.
func (idx *Unique) All(opts *QueryOptions) ([][]byte, error) {
	return allValues(idx.Bucket)
}

// AllInRange returns the item keys corresponding to a range of values.
func (idx *Unique) AllInRange(min, max []byte, opts *QueryOptions) ([][]byte, error) {
	return idx.allInRange(min, max, valueMap)
}
