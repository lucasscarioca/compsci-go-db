package main

const (
	ColumnInt = 1 << iota
	ColumnVarchar
)

type Column struct {
	Name string
	Type int
}
