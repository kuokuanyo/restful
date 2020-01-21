package repository

import (
	"database/sql"

	"github.com/jinzhu/gorm"
)

//Raw : execute sql command
func (r Repository) Raw(DB *gorm.DB, describe string) (*sql.Rows, error) {
	rows, err := DB.Raw(describe).Rows()
	if err != nil {
		return nil, err
	}
	return rows, nil
}
