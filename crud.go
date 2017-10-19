package db

import (
	"github.com/boltdb/bolt"
	"toba.io/lib/db/index"
	"toba.io/lib/db/key"
	"toba.io/lib/db/store"
)

type (
	bucketCallback func(*bolt.Bucket) error
	txCallback     func(*bolt.Tx) error
)

// SystemHas indicates whether the system data file contains a value.
func SystemHas(v store.Value) (bool, error) {
	return Has(SystemFile, v)
}

func SystemHasKey(bucketName, key []byte) (bool, error) {
	return HasKey(SystemFile, bucketName, key)
}

// SystemAdd adds a value to the system data file.
func SystemAdd(v store.Value) ([]byte, error) {
	return Add(SystemFile, v)
}

// TenantHas indicates whether a tenant data file contains a value.
func TenantHas(tenantID []byte, v store.Value) (bool, error) {
	if !Ready {
		return false, ErrNotInitialized
	}
	if !key.IsValid(tenantID) {
		return false, ErrInvalidTenant
	}
	name := key.ToString(tenantID)
	if !validFileName.MatchString(name) {
		return false, ErrInvalidDataFileName
	}

	err := readBucket(rootPath+name, v.BucketName(), func(b *bolt.Bucket) error {
		return nil
	})

	if err != nil {
		return false, err
	}
	return false, nil
}

// Has indicates if data file has value.
func Has(f DataFile, v store.Value) (bool, error) {
	if !Ready {
		return false, ErrNotInitialized
	}
	err := readBucket(path[f], v.BucketName(), func(b *bolt.Bucket) error {
		return nil
	})
	if err != nil {
		return false, err
	}
	return false, nil
}

// HasKey indicates if a bucket contains a key.
func HasKey(f DataFile, bucketName, key []byte) (bool, error) {
	if !Ready {
		return false, ErrNotInitialized
	}
	exists := false
	err := readBucket(path[f], bucketName, func(b *bolt.Bucket) error {
		value := b.Get(key)
		if value != nil {
			exists = true
		}
		return nil
	})
	return exists, err
}

// Get a value from the data file based on an example value. The store.Value
// interface builds a lookup plan utilizing indexes as appropriate.
func Get(f DataFile, v store.Value) (store.Value, error) {
	if !Ready {
		return nil, ErrNotInitialized
	}
	var out store.Value

	err := readBucket(path[f], v.BucketName(), func(b *bolt.Bucket) error {
		//plan := v.QueryPlan(v)

		// if len(plan.Indexes) > 0 {
		// 	idx := plan.Indexes[0]
		// 	itemKey := getIndexedValue(b.Tx(), idx)
		// 	if itemKey == nil {
		// 		return nil
		// 	}
		// }

		data := b.Get([]byte("item key"))
		return Decode(data, out)
	})

	if err != nil {
		return nil, err
	}
	return out, nil
}

// Add a value and its indexes to a data file. The indexes are defined by the
// store.Value interface.
func Add(f DataFile, v store.Value) ([]byte, error) {
	if !Ready {
		return nil, ErrNotInitialized
	}
	k, err := key.Create()
	if err != nil {
		return nil, err
	}
	return k, save(path[f], k, v)
}

// Update an existing value in a data file.
func Update(f DataFile, k []byte, v store.Value) error {
	return save(path[f], k, v)
}

func readBucket(p string, name []byte, fn bucketCallback) error {
	return getBucket(p, name, false, fn)
}

func writeBucket(p string, name []byte, fn bucketCallback) error {
	return getBucket(p, name, true, fn)
}

func getBucketForItem(p string, v store.Value, writable bool, fn bucketCallback) error {
	return getBucket(p, v.BucketName(), writable, fn)
}

// getBucket retrieves the bucket for a value type in the data file at a given
// path. If the bucket should be writable then it will be created if it does
// not already exist, otherwise an error is returned for a non-existent bucket.
func getBucket(p string, name []byte, writable bool, fn bucketCallback) error {
	return withTransaction(p, writable, func(tx *bolt.Tx) error {
		var bucket *bolt.Bucket
		var err error

		if writable {
			bucket, err = tx.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
		} else {
			bucket = tx.Bucket(name)
		}

		if bucket == nil {
			return ErrNoBucket
		}
		return fn(bucket)
	})
}

// withTransaction creates a transaction and passes it to callback function.
func withTransaction(p string, writable bool, fn txCallback) error {
	db, err := openPath(p)
	if err != nil {
		return err
	}
	tx, err := db.Begin(writable)
	if err != nil {
		return err
	}
	defer db.Close()

	if writable {
		defer tx.Rollback()
	}
	err = fn(tx)

	if writable && err == nil {
		return tx.Commit()
	}
	return err
}

// func getIndexedValue(tx *bolt.Tx, idx query.UseIndex) []byte {
// 	bucket := tx.Bucket(idx.IndexBucket)
// 	if bucket == nil {
// 		return nil
// 	}
// 	return bucket.Get(idx.IndexKey)
// }

// save value in Bolt.
// See https://github.com/boltdb/bolt
func save(p string, key []byte, v store.Value) error {
	data, err := Encode(v)
	if err != nil {
		return err
	}

	return withTransaction(p, true, func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(v.BucketName())
		if err != nil {
			return err
		}
		err = bucket.Put(key, data)
		if err != nil {
			return err
		}
		return saveIndexes(key, v.IndexMap(), tx)
	})
}

func saveIndexes(itemKey []byte, indexes index.Map, tx *bolt.Tx) error {
	if indexes.Definitions == nil || len(indexes.Definitions) == 0 {
		return nil
	}
	var idx index.Index
	var err error

	for _, d := range indexes.Definitions {
		if d.Unique {
			idx, err = index.MakeUnique(tx, d.BucketName)
		} else {
			idx, err = index.MakeNonUnique(tx, d.BucketName)
		}
		if err != nil {
			break
		}
		err = idx.Add(d.Value, itemKey)
		if err != nil {
			break
		}
	}
	return err
}
