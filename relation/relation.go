package relation

import "github.com/toba/pbdb/index"

type (
	OneToMany struct {
		pk index.NonUnique
		fk index.Unique
	}
	ManyToMany struct {
		pk index.NonUnique
		fk index.NonUnique
	}
	OneToOne struct {
		pk index.Unique
		fk index.Unique
	}
)
