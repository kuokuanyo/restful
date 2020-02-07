package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"restful/model"
	"restful/repository"
	"restful/utils"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
)

//DropOneField :drop the given field from the given table.
//@Summary drop the given field from the given table.
//@Tags Field
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param field_name path string true "field name"
//@Param db_password query string true "database engine password"
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name}/_field/{field_name} [delete]
func (c Controller) DropOneField() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			information model.DBInformation
			message     model.Error
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			fieldname   = params["field_name"]
			password    string
			passwords   = r.URL.Query()["db_password"]
			repo        repository.Repository
			sql         string
		)
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Require password"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		row := repo.RowOneData(DB, fmt.Sprintf(`select * from users where db_alias='%s'`, dbalias))
		//scan information
		row.Scan(&information.DBAlias, &information.DBType, &information.DBUserName,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.MaxIdle, &information.MaxOpen)
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(information.DBType) {
		case "mysql":
			sql = fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s;`, tablename, fieldname)
		case "mssql":
			sql = fmt.Sprintf(`ALTER TABLE %s.dbo.%s DROP COLUMN %s`, information.DBName, tablename, fieldname)
		}
		if err = repo.Exec(DB, sql); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully")
	}
}

//UpdateOneField :Update one field by description
//@Summary Update one field by description
//@Tags Field
//@Accept json
//@Produce json
//@Param condition body models.SchemaDescription true "Update the description of schema"
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param field_name path string true "field name"
//@Param db_password query string true "database engine password"
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name}/_field/{field_name} [put]
func (c Controller) UpdateOneField() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			information model.DBInformation
			description model.Description
			message     model.Error
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			fieldname   = params["field_name"]
			password    string
			passwords   = r.URL.Query()["db_password"]
			repo        repository.Repository
			sql         string
		)
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Require password"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//decode condition of create table
		json.NewDecoder(r.Body).Decode(&description)
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		row := repo.RowOneData(DB, fmt.Sprintf(`select * from users where db_alias='%s'`, dbalias))
		//scan information
		row.Scan(&information.DBAlias, &information.DBType, &information.DBUserName,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.MaxIdle, &information.MaxOpen)
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(information.DBType) {
		case "mysql":
			sql = fmt.Sprintf(`ALTER TABLE %s MODIFY COLUMN %s %s;`, tablename, fieldname, description.Condition)
		case "mssql":
			sql = fmt.Sprintf(`alter table %s.dbo.%s ALTER COLUMN %s %s`, information.DBName, tablename, fieldname, description.Condition)
		}
		if err = repo.Exec(DB, sql); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully")
	}
}

//AddOneField :create a table field
//@Summary create a table field
//@Tags Field
//@Accept json
//@Produce json
//@Param condition body models.SchemaDescription true "description of field"
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param field_name path string true "field name"
//@Param db_password query string true "database engine password"
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name}/_field/{field_name} [post]
func (c Controller) AddOneField() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			information model.DBInformation
			description model.Description
			message     model.Error
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			fieldname   = params["field_name"]
			password    string
			passwords   = r.URL.Query()["db_password"]
			repo        repository.Repository
			sql         string
		)
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Require password"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//decode condition of create table
		json.NewDecoder(r.Body).Decode(&description)
		//get informations from db_alias
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		row := repo.RowOneData(DB, fmt.Sprintf(`select * from users where db_alias='%s'`, dbalias))
		//scan information
		row.Scan(&information.DBAlias, &information.DBType, &information.DBUserName,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.MaxIdle, &information.MaxOpen)
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(information.DBType) {
		case "mysql":
			sql = fmt.Sprintf(`alter table %s add %s %s`, tablename, fieldname, description.Condition)
		case "mssql":
			sql = fmt.Sprintf(`alter table %s.dbo.%s add %s %s`, information.DBName, tablename, fieldname, description.Condition)
		}
		if err = repo.Exec(DB, sql); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully")
	}
}

//GetOneField :get the definition of given field from the given table
//@Summary get the definition of given field for the given table
//@Tags Field
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param field path string true "field name"
//@Param db_password query string true "database engine password"
//@Success 200 {object} models.FieldStructure "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name}/_field/{field_name} [get]
func (c Controller) GetOneField() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			information model.DBInformation
			message     model.Error
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			fieldname   = params["field_name"]
			password    string
			passwords   = r.URL.Query()["db_password"]
			repo        repository.Repository
			field       model.FieldStructure
		)
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Require password"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//get informations from db_alias
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		row := repo.RowOneData(DB, fmt.Sprintf(`select * from users where db_alias='%s'`, dbalias))
		//scan information
		row.Scan(&information.DBAlias, &information.DBType, &information.DBUserName,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.MaxIdle, &information.MaxOpen)
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			sql := fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME  from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
				information.DBName, tablename, fieldname)
			row = repo.RowOneData(DB, sql)
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			sql := fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME from %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
				information.DBName, tablename, fieldname)
			row = repo.RowOneData(DB, sql)
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//scan information
		row.Scan(&field.TableCatalog, &field.TableSchema, &field.TableName, &field.ColumnName, &field.OrdinalPosition,
			&field.ColumnDefault, &field.IsNullable, &field.DataType, &field.CharacterMaximumLength, &field.CharacterOctetLength,
			&field.NumericPrecision, &field.NumericScale, &field.DatetimePrecision, &field.CharacterSetName, &field.CollationName)
		utils.SendSuccess(w, field)
	}
}
