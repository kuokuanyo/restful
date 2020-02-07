package repository

import (
	"database/sql"

	"github.com/jinzhu/gorm"
)

//Rowmanydata : execute sql command
func (r Repository) Rowmanydata(DB *gorm.DB, describe string) (*sql.Rows, error) {
	rows, err := DB.Raw(describe).Rows()
	if err != nil {
		return nil, err
	}
	return rows, nil
}

//RowOneData : execute sql command
func (r Repository) RowOneData(DB *gorm.DB, describe string) *sql.Row {
	row := DB.Raw(describe).Row()
	return row
}
