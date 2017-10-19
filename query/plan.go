// Package query defines methods for retrieving values from a data file.
package query

// Plan defines the most efficient lookups for retrieving an item from BoltDB.
type (
	Comparison int

	UseIndex struct {
		IndexBucket []byte
		IndexKey    []byte
	}

	Plan struct {
		ItemBucket []byte
		ItemKey    []byte
		Indexes    []UseIndex
	}
)

func Bucket(name []byte) *Plan {
	return &Plan{
		ItemBucket: name,
	}
}

func (b *Plan) UseIndex(name, key []byte) {
	b.Indexes = append(b.Indexes, UseIndex{
		IndexBucket: name,
		IndexKey:    key,
	})
}
