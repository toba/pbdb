package query

import "go/token"

type (
	Query struct {
		Item        interface{}
		Comparisons []*comparison
	}

	comparison struct {
		query    *Query
		Field    string
		Operator int
		op       token.Token
		Target   interface{}
		TargetIn []interface{}
	}
)

func (q *Query) First() interface{} {
	return nil
}

func (q *Query) Field(name string) *comparison {
	c := &comparison{
		query: q,
		Field: name,
	}
	q.Comparisons = append(q.Comparisons, c)
	return c
}

func (c *comparison) Is(target interface{}) *Query {
	return c.compare(0, target)
}

func (c *comparison) In(target ...interface{}) *Query {
	return c.compare(0, target)
}

func (c *comparison) compare(op int, target interface{}) *Query {
	c.Operator = op
	c.Target = target
	return c.query
}
