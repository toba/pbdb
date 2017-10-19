package db_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"toba.io/lib/config"
	"toba.io/lib/db"
	"toba.io/lib/db/index"
)

type TestSchema struct {
	Name      string
	Subdomain string
}

var (
	bucketName = []byte("TestBucket")
	indexName  = index.Name("TestName")
)

func (t *TestSchema) BucketName() []byte { return bucketName }
func (t *TestSchema) IndexMap() index.Map {
	return index.Define([]byte(t.Name), indexName, true)
}

func TestMain(m *testing.M) {
	cfg := config.Database{
		Path: "",
		Name: "test.db",
	}
	dir, _ := ioutil.TempDir(os.TempDir(), "toba")

	defer os.RemoveAll(dir)

	cfg.Path = dir
	db.Initialize(cfg)

	result := m.Run()

	db.Reset()
	os.Exit(result)
}

func TestSystemHasKey(t *testing.T) {
	schema := &TestSchema{Name: "Name", Subdomain: "Subdomain"}
	key, err := db.SystemAdd(schema)
	assert.NoError(t, err)
	assert.NotNil(t, key)

	exists, err := db.SystemHasKey([]byte("NoBucket"), nil)
	assert.Error(t, err)
	assert.False(t, exists)

	exists, err = db.SystemHasKey(bucketName, key)
	assert.NoError(t, err)
	assert.True(t, exists)
}
