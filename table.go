package main

type Table struct {
	Name    string
	columns []Column
	rows    [][]any
}

func NewTable(name string, cols []Column) Table {
	t := Table{
		Name:    name,
		columns: cols,
	}
	return t
}

func (t *Table) Insert(data ...any) {
	t.rows = append(t.rows, data)
}
