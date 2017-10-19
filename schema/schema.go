package schema

import "toba.io/lib/db/index"

var (
	employeeBucketName = []byte("EmployeeBucket")
	personBucketName   = []byte("PersonBucket")
)

type (
	// Address is the physical street address of a person or organization.
	Address struct {
		Street          string `json:"street"`
		City            string `json:"city"`
		StateOrProvince string `json:"stateOrProvince"`
		Country         string `json:"country"`
		PostalCode      string `json:"postalCode"`
	}

	// Employee is a person employed by an organization.
	Employee struct {
		Person
		Number string `json:"number"`
	}

	// Module represents an application module and is used to track licensing.
	Module struct {
		Path string
	}

	Organization struct {
		Name string `json:"name"`
	}

	// Person is a single individual.
	Person struct {
		FirstName string `json:"firstName"`
		LastName  string `json:"lastName"`
	}

	Credentials struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	// Tags are arbitrary words applied to an item.
	Tags []string

	// Unit is a standard of empirical measurement.
	Unit struct {
		Name string
	}

	UnitConversion struct {
		Factor float32
		To     *Unit
	}

	// Writing is written content that might be a response to other written content.
	Writing struct {
		Content    string
		Author     Person
		ResponseTo *Writing
	}
)

func (e *Employee) IndexMap() index.Map {
	return index.Map{}
}

func (e *Employee) BucketName() []byte {
	return employeeBucketName
}

func (e *Person) IndexMap() index.Map {
	return index.Map{}
}

func (e *Person) BucketName() []byte {
	return personBucketName
}
