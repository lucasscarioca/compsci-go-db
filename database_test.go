package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateDatabase(t *testing.T) {
	db := NewDb()

	db.CreateTable("users", []Column{
		{"id", ColumnInt},
		{"name", ColumnVarchar},
		{"email", ColumnVarchar},
		{"password", ColumnVarchar},
	})

	assert.Equal(t, len(db.tables), 1)
}
