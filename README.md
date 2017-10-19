# pbdb
If you have an immediate need for a Go embeddable, [Protobuf](https://developers.google.com/protocol-buffers/) native data store, I recommend [Storm](https://github.com/asdine/storm), which runs on [Bolt](https://github.com/boltdb/bolt) and supports several codecs. Or if Go is not a requirement then [ProfaneDB](https://profanedb.gitlab.io/) has the same objectives as this project.

This project will be the intersection of Storm and ProfaneDB.

- Your [gRPC](https://grpc.io/) compatible Protobuf definitions will dictate the storage schema without Go specific tags or structures.
- Rather than Bolt, the newer [dgraph-io/badger](https://github.com/dgraph-io/badger) store will be used for it's notable [performance advantage](https://blog.dgraph.io/post/badger-lmdb-boltdb/).
- Instead of an ORM with SQL-like methods that depend on reflection, consider plugging into the existing [Go protoc pipline](https://github.com/golang/protobuf/tree/master/protoc-gen-go) to simultaneously generate type safe accessors (work we've [already done elsewhere](https://github.com/toba/wsrpc)) for search.

For project status, see the [issues and milestones](https://github.com/toba/pbdb/issues).
