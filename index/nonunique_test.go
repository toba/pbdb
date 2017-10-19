package index_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"toba.io/lib/db/index"
)

// repeats maps value keys to multiple item keys. The index of the outer
// array is the value key. The inner arrays contain item keys.
var repeats = [][]int{
	nil,            // value[0] = item[0]
	[]int{2, 3, 4}, // value[1] = item[1,2,3,4]
	nil,            // value[2] = item[2]
	[]int{4, 6},    // value[3] = item[3,4,6]
}

// addRepeatItems inserts multiple item keys per value key.
func addRepeatItems(idx *index.NonUnique) error {
	if err := addItems(idx); err != nil {
		return err
	}

	for valueKey, list := range repeats {
		if list == nil {
			continue
		}
		for _, itemKey := range list {
			//println(string(values[valueKey]) + ":" + db.KeyToString(items[itemKey]))
			if err := idx.Add(values[valueKey], items[itemKey]); err != nil {
				return err
			}
		}
	}

	return nil
}

// withNonUnique creates test index with values that are cleaned up after use.
func withNonUnique(t *testing.T, fn func(idx *index.NonUnique)) {
	dir, c, err := connect()
	assert.NoError(t, err)

	defer os.RemoveAll(dir)
	defer c.Close()

	c.Writer(func() error {
		idx, err := c.MakeNonUniqueIndex("test")
		assert.NoError(t, err)

		err = addRepeatItems(idx)
		assert.NoError(t, err)

		fn(idx)

		return nil
	})
}

func TestNonUniqueAdd(t *testing.T) {
	withNonUnique(t, func(idx *index.NonUnique) {
		// no error to add the same index item again
		err := idx.Add(values[2], items[2])
		assert.NoError(t, err)

		// adding second item to same value key should not cause an error
		err = idx.Add(values[3], items[5])
		assert.NoError(t, err)

		// should now be four items indexed to value 3
		matches, _ := idx.AllWithValue(values[3], nil)
		assert.Len(t, matches, 4)
	})
}

func TestNonUniqueRemoveValue(t *testing.T) {
	withNonUnique(t, func(idx *index.NonUnique) {
		err := idx.RemoveValue(values[3])
		assert.NoError(t, err)

		// should be no items left
		matches, err := idx.AllWithValue(values[3], nil)
		assert.NoError(t, err)
		assert.Nil(t, matches)

		key := idx.FirstWithValue(values[3])
		assert.Nil(t, key)

		// removing a non-existent value is okay
		err = idx.RemoveValue([]byte("nothing"))
		assert.NoError(t, err)
	})
}

func TestNonUniqueRemoveItem(t *testing.T) {
	withNonUnique(t, func(idx *index.NonUnique) {
		// initially four items indexed to value 1
		matches, err := idx.AllWithValue(values[1], nil)
		assert.NoError(t, err)
		assert.Len(t, matches, 4)

		err = idx.RemoveItem(items[2])
		assert.NoError(t, err)

		// should be no items left for value 2
		matches, err = idx.AllWithValue(values[2], nil)
		assert.NoError(t, err)
		assert.Nil(t, matches)

		// should be three items still indexed to value 1
		matches, err = idx.AllWithValue(values[1], nil)
		assert.NoError(t, err)
		assert.Len(t, matches, 3)

		// removing a non-existent item is okay
		err = idx.RemoveItem([]byte("nothing"))
		assert.NoError(t, err)
	})
}

func TestNonUniqueFirstWithValue(t *testing.T) {
	withNonUnique(t, func(idx *index.NonUnique) {
		key := idx.FirstWithValue(values[1])
		assert.Equal(t, items[1], key)

		key = idx.FirstWithValue(values[4])
		assert.Equal(t, items[4], key)
	})
}

func TestNonUniqueAllWithValue(t *testing.T) {
	withNonUnique(t, func(idx *index.NonUnique) {
		matches, err := idx.AllWithValue(values[3], nil)
		assert.NoError(t, err)
		assert.Len(t, matches, 3)
		assert.Equal(t, items[4], matches[1])

		matches, err = idx.AllWithValue([]byte("nothing"), nil)
		assert.NoError(t, err)
		assert.Nil(t, matches)
	})
}

func TestNonUniqueAll(t *testing.T) {
	withNonUnique(t, func(idx *index.NonUnique) {
		matches, err := idx.All(nil)
		assert.NoError(t, err)
		assert.Len(t, matches, len(items))
	})
}

func TestNonUniqueAllInRange(t *testing.T) {
	withNonUnique(t, func(idx *index.NonUnique) {
		matches, err := idx.AllInRange(values[0], values[1], nil)
		assert.NoError(t, err)
		assert.Len(t, matches, 5)

		matches, err = idx.AllInRange(values[5], values[7], nil)
		assert.NoError(t, err)
		assert.Len(t, matches, 3)

		// six matches but item[4] is duplicated so five distinct
		matches, err = idx.AllInRange(values[2], values[5], nil)
		assert.NoError(t, err)
		assert.Len(t, matches, 5)
	})
}
