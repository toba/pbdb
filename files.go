package pbdb

import (
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

// OpenFiles is a thread-safe map of open database files.
type OpenFiles struct {
	sync.RWMutex
	files map[string]*bolt.DB
}

// Connect opens or creates a bolt data file at given path.
func (o OpenFiles) Connect(path string) (*bolt.DB, error) {
	if !o.Has(path) {
		db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			return nil, err
		}
		o.Add(path, db)
	}
	return o.Get(path), nil
}

func (o OpenFiles) Add(path string, db *bolt.DB) {
	o.Lock()
	o.files[path] = db
	o.Unlock()
}

func (o OpenFiles) Remove(path string) {
	o.Lock()
	o.Close(path)
	delete(o.files, path)
	o.Unlock()
}

func (o OpenFiles) Get(path string) *bolt.DB {
	o.RLock()
	db, ok := o.files[path]
	o.RUnlock()
	if ok {
		return db
	}
	return nil
}

func (o OpenFiles) Has(path string) bool {
	o.RLock()
	_, ok := o.files[path]
	o.RUnlock()
	return ok
}

// CloseAll closes all data file connections
// TODO: check if db is open before closing
func (o OpenFiles) CloseAll() {
	o.RLock()
	for _, db := range o.files {
		db.Close()
	}
	o.RUnlock()
}

// Close data file at given path
// TODO: check if db is open before closing
func (o OpenFiles) Close(path string) {
	if db, ok := o.files[path]; ok {
		db.Close()
	}
}
