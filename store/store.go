// Package store defines the shape of data to be stored in the database.
package store

import "github.com/toba/pbdb/index"

type (
	// Item is a key and value. The value is a struct matching the
	// store.Value interface.
	Item struct {
		Key []byte

		Value Value
	}

	// Value defines methods that must be implemented by stored models.
	Value interface {
		// IndexMap should return values to be indexed mapped to the name of
		// their index.
		//
		// Field tags could accomplish something similar but having an explicit
		// method supports opions like indexing on a combination of fields.
		IndexMap() index.Map

		// BucketName returns the name of the bucket to use for storing a model.
		BucketName() []byte

		// QueryPlan builds the most efficient plan to retrieve a value matching
		// an example.
		//QueryPlan(Value) query.Plan
	}
)
