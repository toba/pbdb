package key_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"toba.io/lib/db/key"
)

var (
	tinyKey  = []byte{1, 2}
	shortKey = []byte{1, 2, 3, 4, 5}
	ulidKey  = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	longKey  = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
)

func TestCreate(t *testing.T) {
	key1, err := key.Create()
	key2, err := key.Create()
	key3, err := key.Create()
	assert.NoError(t, err)
	assert.Len(t, key1, 16)

	assert.NotEqual(t, key1, key2)
	assert.NotEqual(t, key1, key3)
}

// TestZero ensures the empty key always sorts before others
// so it can be used to seek the first of a range of prefixes.
func TestZeroAndMax(t *testing.T) {
	assert.Len(t, key.Zero, 16)
	assert.Len(t, key.Max, 16)

	key1, err := key.Create()
	time.Sleep(time.Millisecond)
	key2, err := key.Create()

	assert.NoError(t, err)

	assert.Equal(t, -1, bytes.Compare(key.Zero, key1))
	assert.Equal(t, -1, bytes.Compare(key.Zero, key2))

	assert.Equal(t, 1, bytes.Compare(key.Max, key1))
	assert.Equal(t, 1, bytes.Compare(key.Max, key2))
}

func TestIsValid(t *testing.T) {
	assert.False(t, key.IsValid(shortKey))
	assert.False(t, key.IsValid(nil))
	assert.True(t, key.IsValid(ulidKey))
	assert.False(t, key.IsValid(longKey))
}

func TestIsEmpty(t *testing.T) {
	assert.False(t, key.IsEmpty(shortKey))
	assert.True(t, key.IsEmpty(nil))
	assert.False(t, key.IsEmpty(longKey))
}

// TestKeySort verifies that keys are generated in sorted order. Sort order is
// not guaranteed for keys created in the same millisecond.
//
// See https://github.com/alizain/ulid#sorting
func TestKeySort(t *testing.T) {
	key1, err := key.Create()
	time.Sleep(time.Millisecond)
	key2, err := key.Create()
	time.Sleep(time.Millisecond)
	key3, err := key.Create()
	time.Sleep(time.Millisecond)
	key4, err := key.Create()

	assert.NoError(t, err)
	assert.Equal(t, -1, bytes.Compare(key1, key2))
	assert.Equal(t, -1, bytes.Compare(key3, key4))
}

func TestParse(t *testing.T) {
	k, err := key.Create()
	assert.NoError(t, err)
	text := key.ToString(k)
	assert.NotEmpty(t, text)
	match := key.FromString(text)
	assert.Equal(t, k, match)
}

func TestListContains(t *testing.T) {
	key1, err := key.Create()
	time.Sleep(time.Millisecond)
	key2, err := key.Create()
	time.Sleep(time.Millisecond)
	key3, err := key.Create()
	time.Sleep(time.Millisecond)
	key4, err := key.Create()

	assert.NoError(t, err)

	list := [][]byte{key1, key2, key3}

	assert.True(t, key.ListContains(list, key2))
	assert.False(t, key.ListContains(list, key4))
}

func TestMergeLists(t *testing.T) {
	key1, err := key.Create()
	time.Sleep(time.Millisecond)
	key2, err := key.Create()
	time.Sleep(time.Millisecond)
	key3, err := key.Create()
	time.Sleep(time.Millisecond)
	key4, err := key.Create()

	assert.NoError(t, err)

	list1 := [][]byte{key1, key2, key3}
	list2 := [][]byte{key3, key4}
	list3 := [][]byte{key1}

	merged := key.MergeLists(list1, list2, list3)
	assert.Len(t, merged, 4, "Merged list has %d items instead of %d", len(list1), 4)

	emptyList1 := [][]byte{}
	emptyList2 := [][]byte{}

	// should return nil instead of empty list
	merged = key.MergeLists(emptyList1, emptyList2)
	assert.Nil(t, merged)

	// handle empty or nil lists
	merged = key.MergeLists(emptyList1, list2, nil)
	assert.Len(t, merged, 2)
}
