package index_test

import (
	"io/ioutil"
	"os"

	"toba.io/lib/db"
	"toba.io/lib/db/key"
)

var (
	// item keys are stored as the values in an index
	items = [][]byte{
		key.FromString("01B8NDW533F20DBGJVEDSHW3BZ"),
		key.FromString("01B8NDW533J6NQMYM9T10B1K3C"),
		key.FromString("01B8NDW533KBPAZ0NDHEKWEDRX"),
		key.FromString("01B8NDW533MP71DEHZD7AV97D1"),
		key.FromString("01B8NDW533SKQ86B97Y8H888C8"),
		key.FromString("01B8NDW533SKRR70MF8F7548MH"),
		key.FromString("01B8NDW533TSVGB126Q71TSQBY"),
		key.FromString("01B8NDW533WWDAKNXKDRJ87RTZ"),
		key.FromString("01B8NDW533Z1X4VPV9AJ6F0SDM"),
		key.FromString("01B8NE6WJ0MJ8N4QQAJZ6GNG7S"),
	}

	// item value become the keys
	values = [][]byte{
		[]byte("aa zero"),
		[]byte("bb one"),
		[]byte("cc two"),
		[]byte("dd three"),
		[]byte("ee four"),
		[]byte("ff five"),
		[]byte("gg six"),
		[]byte("hh seven"),
		[]byte("ii eight"),
		[]byte("jj nine"),
	}
)

func addItems(idx Index) error {
	for i := 0; i < 10; i++ {
		err := idx.Add(values[i], items[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func connect() (string, *db.Client, error) {
	dir, err := ioutil.TempDir(os.TempDir(), "toba")
	if err != nil {
		return dir, &db.Client{IsConnected: false}, err
	}
	path := dir + string(os.PathSeparator) + "test.db"
	c, err := db.Connect(path)

	return dir, c, err
}
