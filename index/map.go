package index

type (
	// Definition of an indexed value.
	Definition struct {
		// BucketName for the index.
		BucketName []byte
		// Value to be indexed.
		Value []byte
		// Unique indicates a unique index should be used, otherwise a non-
		// unique index is used.
		Unique bool
	}

	// Map matches values to be indexed and the index type with an index name.
	Map struct {
		Definitions []*Definition
	}
)

// Define creates an index definition.
func Define(value, name []byte, unique bool) Map {
	return Map{
		Definitions: []*Definition{&Definition{
			BucketName: name,
			Value:      value,
			Unique:     unique,
		}},
	}
}

func (m Map) Add(value, name []byte, unique bool) Map {
	m.Definitions = append(m.Definitions, &Definition{
		BucketName: name,
		Value:      value,
		Unique:     unique,
	})
	return m
}
