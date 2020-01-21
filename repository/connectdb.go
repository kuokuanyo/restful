package repository

import (
	"github.com/jinzhu/gorm"
)

//ConnectDb :connect db
func (r Repository) ConnectDb(engine, source string) (*gorm.DB, error) {
	DB, err := gorm.Open(engine, source)
	if err != nil {
		return nil, err
	}
	return DB, nil
}
