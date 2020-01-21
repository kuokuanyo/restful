package repository

import (
	"github.com/jinzhu/gorm"
)

//Exec :execute sql command
func (r Repository) Exec(DB *gorm.DB, sql string) error {
	if err := DB.Exec(sql).Error; err != nil {
		return err
	}
	return nil
}
