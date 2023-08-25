package main

type Database struct {
	tables  map[string]Table
	indexes map[string]string
}

func NewDb() *Database {
	return &Database{
		tables: make(map[string]Table),
	}
}

func (d *Database) CreateTable(name string, cols []Column) *Table {
	t := NewTable(name, cols)
	d.tables[t.Name] = t
	return &t
}
