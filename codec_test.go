package db_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/toba/pbdb"
	"github.com/toba/pbdb/schema"
)

var (
	signed = map[int][]byte{
		-123456:            []byte{255, 255, 255, 255, 255, 254, 29, 192},
		0:                  []byte{0, 0, 0, 0, 0, 0, 0, 0},
		56:                 []byte{0, 0, 0, 0, 0, 0, 0, 56},
		123456789:          []byte{0, 0, 0, 0, 7, 91, 205, 21},
		123456789123456789: []byte{1, 182, 155, 75, 172, 208, 95, 21},
	}

	unsigned = map[uint][]byte{
		1984: []byte{0, 0, 0, 0, 0, 0, 7, 192},
		2017: []byte{0, 0, 0, 0, 0, 0, 7, 225},
	}
)

func TestEncodeDecode(t *testing.T) {
	buf, err := db.Encode(employee.Value)
	assert.NoError(t, err)
	assert.NotNil(t, buf)

	out := &schema.Employee{}
	err = db.Decode(buf, out)
	assert.NoError(t, err)
	assert.NotNil(t, out)
	assert.Equal(t, employeeNumber, out.Number)
	assert.Equal(t, firstName, out.FirstName)
}

func TestNumberToBytes(t *testing.T) {
	for num, expect := range signed {
		buf, err := db.NumberToBytes(num)
		assert.NoError(t, err)
		assert.Equal(t, buf, expect)
	}

	for num, expect := range unsigned {
		buf, err := db.NumberToBytes(num)
		assert.NoError(t, err)
		assert.Equal(t, buf, expect)
	}

	buf, err := db.NumberToBytes("notanumber")
	assert.Error(t, err)
	assert.Nil(t, buf)
}

func TestNumberFromBytes(t *testing.T) {
	for expect, buf := range signed {
		num, err := db.NumberFromBytes(buf)
		assert.NoError(t, err)
		assert.Equal(t, num, int64(expect))
	}
}
