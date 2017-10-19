package index_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func writer(t *testing.T, fn func() error) {
	connect(t, func(c *client.Client) {
		c.Writer(fn)
	})
}

func TestMakeUniqueIndex(t *testing.T) {
	connect(t, func(c *client.Client) {
		c.Writer(func() error {
			idx, err := c.MakeUniqueIndex("name")
			assert.NoError(t, err)
			assert.NotNil(t, idx)
			assert.NotNil(t, idx.Bucket)
			assert.True(t, idx.Bucket.Writable())

			return nil
		})
	})
}

func TestMakeNonUniqueIndex(t *testing.T) {
	connect(t, func(c *client.Client) {
		c.Writer(func() error {
			idx, err := c.MakeNonUniqueIndex("name")
			assert.NoError(t, err)
			assert.NotNil(t, idx)
			assert.NotNil(t, idx.Bucket)
			assert.True(t, idx.Bucket.Writable())

			return nil
		})
	})
}

func TestMakeRelation(t *testing.T) {
	connect(t, func(c *client.Client) {
		c.Writer(func() error {
			idx, err := c.MakeRelation("name")
			assert.NoError(t, err)
			assert.NotNil(t, idx)
			assert.NotNil(t, idx.Bucket)
			assert.True(t, idx.Bucket.Writable())

			return nil
		})
	})
}
