package db_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"toba.io/lib/config"
	"toba.io/lib/db"
	"toba.io/lib/db/key"
	"toba.io/lib/db/schema"
	"toba.io/lib/db/store"
)

const (
	employeeNumber = "N1234"
	firstName      = "First"
	lastName       = "Last"
)

var (
	employee = &store.Item{
		Key: []byte{1, 2, 3, 4},
		Value: &schema.Employee{
			Number: employeeNumber,
			Person: schema.Person{
				FirstName: firstName,
				LastName:  lastName,
			},
		},
	}
)

func TestUninitialized(t *testing.T) {
	db.Reset()

	t.Run("Uninitialized", func(t *testing.T) {
		_, err := db.Open(db.SystemFile)
		assert.Equal(t, db.ErrNotInitialized, err)
	})
}

func TestConnection(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "toba")

	assert.NoError(t, err)
	defer os.Remove(dir)

	_, err = os.Stat(dir)
	assert.NoError(t, err)

	name := "test.db"

	db.Initialize(config.Database{
		Path: dir,
		Name: name,
	})

	t.Run("File Name", func(t *testing.T) {
		_, err := db.OpenFile("invalid/name")
		assert.Equal(t, db.ErrInvalidDataFileName, err)

		_, err = db.OpenFile("invalid...")
		assert.Equal(t, db.ErrInvalidDataFileName, err)

		id, err := key.Create()
		assert.NoError(t, err)

		cn, err := db.OpenFile(key.ToString(id))
		assert.NoError(t, err)
		cn.Close()
	})
}
