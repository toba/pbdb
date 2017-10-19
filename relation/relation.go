package relation

import "toba.io/lib/db/index"

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
