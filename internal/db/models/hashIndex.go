package models

type HashIndexFindResponse struct {
	PageAddr     int
	PagePosition int
	Tuple        any
}

type HashIndexStatsResponse struct {
	NR             int
	FR             int
	NB             int
	PagesQtty      int
	OverflowCount  int
	OverflowPct    string
	CollisionCount int
	CollisionPct   string
}
