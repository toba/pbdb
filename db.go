// Package db creates and verifies BoltDB database files.
package pbdb

import (
	"toba.io/lib/config"

	"os"

	"strings"

	"regexp"

	"github.com/boltdb/bolt"
)

type DataFile int

const (
	// SystemFile is the path to the data file storing values common to all
	// tenants, such as statistics and licenses. By default it is the
	// root path plus configuration.Database.Name.
	SystemFile DataFile = iota
	// LogFile is the path to the data file that records common application
	// activity. By default it is the root path plus logs.db.
	LogFile
)

var (
	// rootPath is the root directory for storing database files. It is
	// usually specified by configuration.Database.Path.
	rootPath string
	path     map[DataFile]string

	// Ready indicates database files have been created and write access
	// validated.
	Ready = false
	slash = string(os.PathSeparator)

	validFileName = regexp.MustCompile(`^[a-zA-Z0-9]{3,}$`)

	dataFiles = OpenFiles{files: make(map[string]*bolt.DB)}
)

// Open returns connection to specific data file.
func Open(f DataFile) (*bolt.DB, error) {
	if !Ready {
		return nil, ErrNotInitialized
	}
	return dataFiles.Connect(path[f])
}

// OpenFile returns a connection to the named file within the root path.
func OpenFile(name string) (*bolt.DB, error) {
	if !Ready {
		return nil, ErrNotInitialized
	}
	if !validFileName.MatchString(name) {
		return nil, ErrInvalidDataFileName
	}
	return dataFiles.Connect(rootPath + name)
}

// Initialize database directory and files. Operations will use the
// validated paths until re-initialized.
func Initialize(config config.Database) error {
	Reset()
	rootPath = config.Path

	if rootPath != "" {
		// empty path means current directory
		if !strings.HasSuffix(rootPath, slash) {
			rootPath += slash
		}
		_, err := os.Stat(rootPath)

		if err != nil && os.IsNotExist(err) {
			// create path if it doesn't exist
			err = os.Mkdir(rootPath, 0700)
			if err != nil {
				return err
			}
		}
	}

	path[SystemFile] = rootPath + config.Name
	path[LogFile] = rootPath + "logs.db"

	// ensure database file can be opened
	_, err := dataFiles.Connect(path[SystemFile])
	if err == nil {
		Ready = true
	}
	return err
}

func Close() {
	dataFiles.CloseAll()
}

// Reset sets Ready to false and clears data file paths.
func Reset() {
	Close()
	Ready = false
	rootPath = ""
	path = make(map[DataFile]string)
}
