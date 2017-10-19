package index

// QueryOptions are used to customize queries.
type QueryOptions struct {
	Limit   int
	Skip    int
	Reverse bool
}

// NewOptions creates initialized Options
func NewOptions() *QueryOptions {
	return &QueryOptions{
		Limit: -1,
	}
}
