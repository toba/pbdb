package index

import "github.com/boltdb/bolt"

// Index interface defines methods required of all index types, such as
// unique, non-unique and relation indexes.
type Index interface {
	Add(valueKey, itemKey []byte) error
	RemoveItem(itemKey []byte) error
	RemoveValue(valueKey []byte) error
	FirstWithValue(valueKey []byte) []byte
	AllWithValue(valueKey []byte, opts *QueryOptions) ([][]byte, error)
	All(opts *QueryOptions) ([][]byte, error)
	AllInRange(min, max []byte, opts *QueryOptions) ([][]byte, error)
}

// Prefix is arbitrary text added to the beginning of index names
// to avoid conflicts with item bucket names.
const Prefix = "_index_"

// Name creates index bucket name.
func Name(name string) []byte { return []byte(Prefix + name) }

// MakeUnique creates an index that
func MakeUnique(tx *bolt.Tx, indexName []byte) (*Unique, error) {
	bucket, err := tx.CreateBucketIfNotExists(indexName)
	if err != nil {
		return nil, err
	}
	return makeUnique(bucket), nil
}

// UniqueIndex returns a pointer to the named, unique index.
func GetUnique(tx *bolt.Tx, indexName []byte) *Unique {
	bucket := tx.Bucket(indexName)
	if bucket == nil {
		return nil
	}
	return makeUnique(bucket)
}

// MakeNonUniqueIndex creates an index allowing multiple values to reference
// the same item key.
func MakeNonUnique(tx *bolt.Tx, indexName []byte) (*NonUnique, error) {
	bucket, err := tx.CreateBucketIfNotExists(indexName)
	if err != nil {
		return nil, err
	}
	return makeNonUnique(bucket), nil
}

// NonUniqueIndex returns a pointer to the named, non-unique index.
func GetNonUnique(tx *bolt.Tx, indexName []byte) *NonUnique {
	bucket := tx.Bucket(indexName)
	if bucket == nil {
		return nil
	}
	return makeNonUnique(bucket)
}

func makeUnique(b *bolt.Bucket) *Unique {
	return &Unique{
		baseIndex: baseIndex{Bucket: b},
	}
}

func makeNonUnique(b *bolt.Bucket) *NonUnique {
	return &NonUnique{
		baseIndex: baseIndex{Bucket: b},
	}
}

// MakeRelation creates an index relating one item to another.
// func MakeRelation(tx *bolt.Tx, indexName string) (*Relation, error) {
// 	bucket, err := makeIndexBucket(tx, indexName)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &Relation{baseIndex: makeBaseIndex(bucket)}, nil
// }
