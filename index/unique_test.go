package index_test

import (
	"os"
	"testing"

	"toba.io/lib/db/index"
	"toba.io/lib/oops"

	"github.com/stretchr/testify/assert"
)

// withUnique creates test index with values that are removed after use.
func withUnique(t *testing.T, fn func(idx *index.Unique)) {
	dir, c, err := connect()
	assert.NoError(t, err)

	defer os.RemoveAll(dir)
	defer c.Close()

	c.Writer(func() error {
		idx, err := c.MakeUniqueIndex("test")
		assert.NoError(t, err)

		err = addItems(idx)
		assert.NoError(t, err)

		fn(idx)

		return nil
	})
}

func TestUniqueAdd(t *testing.T) {
	withUnique(t, func(idx *index.Unique) {
		// no error to add the same index item again
		err := idx.Add(values[2], items[2])
		assert.NoError(t, err)

		// replacing existing index should return error
		err = idx.Add(values[3], items[5])
		assert.Error(t, oops.AlreadyExists)
	})
}

func TestUniqueFirstWithValue(t *testing.T) {
	withUnique(t, func(idx *index.Unique) {
		key := idx.FirstWithValue(values[2])
		assert.Equal(t, key, items[2])

		// searching for a non-existent value should return nil
		key = idx.FirstWithValue([]byte("nothing"))
		assert.Nil(t, key)
	})
}

func TestUniqueRemoveValue(t *testing.T) {
	withUnique(t, func(idx *index.Unique) {
		err := idx.RemoveValue(values[3])
		assert.NoError(t, err)

		key := idx.FirstWithValue(values[3])
		assert.Nil(t, key)

		// removing a non-existent value is okay
		err = idx.RemoveValue([]byte("nothing"))
		assert.NoError(t, err)
	})
}

func TestUniqueRemoveItem(t *testing.T) {
	withUnique(t, func(idx *index.Unique) {
		err := idx.RemoveItem(items[3])
		assert.NoError(t, err)

		// key should have been removed
		key := idx.FirstWithValue(values[3])
		assert.Nil(t, key)

		// removing a non-existent item is okay
		err = idx.RemoveItem([]byte("nothing"))
		assert.NoError(t, err)
	})
}

func TestUniqueAllWithValue(t *testing.T) {
	withUnique(t, func(idx *index.Unique) {
		matches, err := idx.AllWithValue(values[3], nil)
		assert.NoError(t, err)
		assert.Len(t, matches, 1)
		assert.Equal(t, items[3], matches[0])
	})
}

func TestUniqueAll(t *testing.T) {
	withUnique(t, func(idx *index.Unique) {
		matches, err := idx.All(nil)
		assert.NoError(t, err)
		assert.Len(t, matches, len(items))
	})
}

func TestUniqueAllInRange(t *testing.T) {
	withUnique(t, func(idx *index.Unique) {
		matches, err := idx.AllInRange(values[2], values[4], nil)
		assert.NoError(t, err)
		assert.Len(t, matches, 3)

		matches, err = idx.AllInRange(values[2], values[8], nil)
		assert.NoError(t, err)
		assert.Len(t, matches, 7)
	})
}
