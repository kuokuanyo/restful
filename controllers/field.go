package controllers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jinzhu/gorm"

	"restful/model"
	"restful/repository"
	"restful/utils"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
)

//DropOneField :Remove the given field from the given table.
//@Summary Remove the given field from the given table.
//@Tags Field
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param db_password query string true "database engine password"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param field_name path string true "field name"
//@Param db_password query string true "database engine password"
//@Success 200 {object} model.FieldStructure "Successfully"
//@Failure 401 {object} model.Error "Unauthorized"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name}/_field/{field_name} [delete]
func (c Controller) DropOneField() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			information model.Engine
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			fieldname   = params["field_name"]
			passwords   = r.URL.Query()["db_password"]
			field       model.FieldStructure
			Source      string
			password    string
			sqlorder    string
			err         error
		)

		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Required password."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`,
				dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
				Storing.DBName, dbalias))
		}
		//scan information
		if err = row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}

		//identify db_type
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		case "mssql":
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		}

		DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, 
			ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, 
			CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, 
			COLLATION_NAME  from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' 
			and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
				information.DBName, tablename, fieldname)
		case "mssql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, 
			TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, 
			DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, 
			NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME 
			from %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
				information.DBName, tablename, fieldname)
		}

		row = repo.RowOneData(DB, sqlorder)

		var (
			value     = make([]sql.NullString, 15)
			valuePtrs = make([]interface{}, 15)
		)
		for i := 0; i < 15; i++ {
			valuePtrs[i] = &value[i]
		}

		//scan information
		if err = row.Scan(valuePtrs...); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		field.TableCatalog = value[0].String
		field.TableSchema = value[1].String
		field.TableName = value[2].String
		field.ColumnName = value[3].String
		field.OrdinalPosition = value[4].String
		if value[5].String == "" {
			field.ColumnDefault = "NULL"
		} else {
			field.ColumnDefault = value[5].String
		}
		field.IsNullable = value[6].String
		field.DataType = value[7].String
		if value[8].String == "" {
			field.CharacterMaximumLength = "NULL"
		} else {
			field.CharacterMaximumLength = value[8].String
		}
		if value[9].String == "" {
			field.CharacterOctetLength = "NULL"
		} else {
			field.CharacterOctetLength = value[9].String
		}
		if value[10].String == "" {
			field.NumericPrecision = "NULL"
		} else {
			field.NumericPrecision = value[10].String
		}
		if value[11].String == "" {
			field.NumericScale = "NULL"
		} else {
			field.NumericScale = value[11].String
		}
		if value[12].String == "" {
			field.DatetimePrecision = "NULL"
		} else {
			field.DatetimePrecision = value[12].String
		}
		if value[13].String == "" {
			field.CharacterSetName = "NULL"
		} else {
			field.CharacterSetName = value[13].String
		}
		if value[14].String == "" {
			field.CollationName = "NULL"
		} else {
			field.CollationName = value[14].String
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s;`, tablename, fieldname)
		case "mssql":
			sqlorder = fmt.Sprintf(`ALTER TABLE %s.dbo.%s DROP COLUMN %s`, information.DBName, tablename, fieldname)
		}

		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		utils.SendSuccess(w, field)
	}
}

//UpdateOneField :Update table fields with the given properties.
//@Summary Update table fields with the given properties.
//@Tags Field
//@Accept json
//@Produce json
//@Param condition body model.SchemaDescription true "Update the description of schema"
//@Param db_alias path string true "database engine alias"
//@Param db_password query string true "database engine password"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param field_name path string true "field name"
//@Success 200 {object} model.FieldStructure "Successfully"
//@Failure 401 {object} model.Error "Unauthorized"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name}/_field/{field_name} [put]
func (c Controller) UpdateOneField() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			information model.Engine
			description model.Description
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			fieldname   = params["field_name"]
			passwords   = r.URL.Query()["db_password"]
			field       model.FieldStructure
			Source      string
			password    string
			sqlorder    string
			err         error
		)

		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Required password."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		//decode condition of create table
		json.NewDecoder(r.Body).Decode(&description)

		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`, dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`, Storing.DBName, dbalias))
		}
		//scan information
		if err = row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}

		//identify db_type
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		case "mssql":
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		}

		DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`ALTER TABLE %s MODIFY COLUMN %s %s;`,
				tablename, fieldname, description.Condition)
		case "mssql":
			sqlorder = fmt.Sprintf(`alter table %s.dbo.%s ALTER COLUMN %s %s`,
				information.DBName, tablename, fieldname, description.Condition)
		}

		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, 
			ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, 
			CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, 
			COLLATION_NAME  from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' 
			and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
				information.DBName, tablename, fieldname)
		case "mssql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, 
			TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, 
			DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, 
			NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME 
			from %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
				information.DBName, tablename, fieldname)
		}

		row = repo.RowOneData(DB, sqlorder)

		var (
			value     = make([]sql.NullString, 15)
			valuePtrs = make([]interface{}, 15)
		)
		for i := 0; i < 15; i++ {
			valuePtrs[i] = &value[i]
		}

		//scan information
		if err = row.Scan(valuePtrs...); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		field.TableCatalog = value[0].String
		field.TableSchema = value[1].String
		field.TableName = value[2].String
		field.ColumnName = value[3].String
		field.OrdinalPosition = value[4].String
		if value[5].String == "" {
			field.ColumnDefault = "NULL"
		} else {
			field.ColumnDefault = value[5].String
		}
		field.IsNullable = value[6].String
		field.DataType = value[7].String
		if value[8].String == "" {
			field.CharacterMaximumLength = "NULL"
		} else {
			field.CharacterMaximumLength = value[8].String
		}
		if value[9].String == "" {
			field.CharacterOctetLength = "NULL"
		} else {
			field.CharacterOctetLength = value[9].String
		}
		if value[10].String == "" {
			field.NumericPrecision = "NULL"
		} else {
			field.NumericPrecision = value[10].String
		}
		if value[11].String == "" {
			field.NumericScale = "NULL"
		} else {
			field.NumericScale = value[11].String
		}
		if value[12].String == "" {
			field.DatetimePrecision = "NULL"
		} else {
			field.DatetimePrecision = value[12].String
		}
		if value[13].String == "" {
			field.CharacterSetName = "NULL"
		} else {
			field.CharacterSetName = value[13].String
		}
		if value[14].String == "" {
			field.CollationName = "NULL"
		} else {
			field.CollationName = value[14].String
		}

		utils.SendSuccess(w, field)
	}
}

//AddOneField :Create table fields.
//@Summary Create table fields.
//@Tags Field
//@Accept json
//@Produce json
//@Param condition body model.SchemaDescription true "description of field"
//@Param db_alias path string true "database engine alias"
//@Param db_password query string true "database engine password"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param field_name path string true "field name"
//@Success 200 {object} model.FieldStructure "Successfully"
//@Failure 401 {object} model.Error "Unauthorized"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name}/_field/{field_name} [post]
func (c Controller) AddOneField() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			information model.Engine
			description model.Description
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			fieldname   = params["field_name"]
			passwords   = r.URL.Query()["db_password"]
			field       model.FieldStructure
			Source      string
			password    string
			sqlorder    string
			err         error
		)

		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Required password."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		//decode condition of create table
		json.NewDecoder(r.Body).Decode(&description)

		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`,
				dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
				Storing.DBName, dbalias))
		}
		//scan information
		if err = row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}

		//identify db_type
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		case "mssql":
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		}

		DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`alter table %s add %s %s`, tablename, fieldname, description.Condition)
		case "mssql":
			sqlorder = fmt.Sprintf(`alter table %s.dbo.%s add %s %s`, information.DBName, tablename, fieldname, description.Condition)
		}

		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, 
			ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, 
			CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, 
			COLLATION_NAME  from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' 
			and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
				information.DBName, tablename, fieldname)
		case "mssql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, 
			TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, 
			DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, 
			NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME 
			from %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
				information.DBName, tablename, fieldname)
		}

		row = repo.RowOneData(DB, sqlorder)

		var (
			value     = make([]sql.NullString, 15)
			valuePtrs = make([]interface{}, 15)
		)
		for i := 0; i < 15; i++ {
			valuePtrs[i] = &value[i]
		}

		//scan information
		if err = row.Scan(valuePtrs...); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		field.TableCatalog = value[0].String
		field.TableSchema = value[1].String
		field.TableName = value[2].String
		field.ColumnName = value[3].String
		field.OrdinalPosition = value[4].String
		if value[5].String == "" {
			field.ColumnDefault = "NULL"
		} else {
			field.ColumnDefault = value[5].String
		}
		field.IsNullable = value[6].String
		field.DataType = value[7].String
		if value[8].String == "" {
			field.CharacterMaximumLength = "NULL"
		} else {
			field.CharacterMaximumLength = value[8].String
		}
		if value[9].String == "" {
			field.CharacterOctetLength = "NULL"
		} else {
			field.CharacterOctetLength = value[9].String
		}
		if value[10].String == "" {
			field.NumericPrecision = "NULL"
		} else {
			field.NumericPrecision = value[10].String
		}
		if value[11].String == "" {
			field.NumericScale = "NULL"
		} else {
			field.NumericScale = value[11].String
		}
		if value[12].String == "" {
			field.DatetimePrecision = "NULL"
		} else {
			field.DatetimePrecision = value[12].String
		}
		if value[13].String == "" {
			field.CharacterSetName = "NULL"
		} else {
			field.CharacterSetName = value[13].String
		}
		if value[14].String == "" {
			field.CollationName = "NULL"
		} else {
			field.CollationName = value[14].String
		}

		utils.SendSuccess(w, field)
	}
}

//GetOneField :Retrieve the definition of the given field for the given table.
//@Summary Retrieve the definition of the given field for the given table.
//@Tags Field
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param db_password query string true "database engine password"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param field path string true "field name"
//@Success 200 {object} model.FieldStructure "Successfully"
//@Failure 401 {object} model.Error "Unauthorized"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name}/_field/{field_name} [get]
func (c Controller) GetOneField() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			information model.Engine
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			fieldname   = params["field_name"]
			passwords   = r.URL.Query()["db_password"]
			Source      string
			sqlorder    string
			password    string
			field       model.FieldStructure
			err         error
		)

		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Required password."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`, dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`, Storing.DBName, dbalias))
		}
		//scan information
		if err = row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}

		//identify db_type
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, 
			ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, 
			CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, 
			COLLATION_NAME  from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' 
			and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
				information.DBName, tablename, fieldname)
		case "mssql":
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, 
			TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, 
			DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, 
			NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME 
			from %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
				information.DBName, tablename, fieldname)
		}

		DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		row = repo.RowOneData(DB, sqlorder)

		var (
			value     = make([]sql.NullString, 15)
			valuePtrs = make([]interface{}, 15)
		)
		for i := 0; i < 15; i++ {
			valuePtrs[i] = &value[i]
		}

		//scan information
		if err = row.Scan(valuePtrs...); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		field.TableCatalog = value[0].String
		field.TableSchema = value[1].String
		field.TableName = value[2].String
		field.ColumnName = value[3].String
		field.OrdinalPosition = value[4].String
		if value[5].String == "" {
			field.ColumnDefault = "NULL"
		} else {
			field.ColumnDefault = value[5].String
		}
		field.IsNullable = value[6].String
		field.DataType = value[7].String
		if value[8].String == "" {
			field.CharacterMaximumLength = "NULL"
		} else {
			field.CharacterMaximumLength = value[8].String
		}
		if value[9].String == "" {
			field.CharacterOctetLength = "NULL"
		} else {
			field.CharacterOctetLength = value[9].String
		}
		if value[10].String == "" {
			field.NumericPrecision = "NULL"
		} else {
			field.NumericPrecision = value[10].String
		}
		if value[11].String == "" {
			field.NumericScale = "NULL"
		} else {
			field.NumericScale = value[11].String
		}
		if value[12].String == "" {
			field.DatetimePrecision = "NULL"
		} else {
			field.DatetimePrecision = value[12].String
		}
		if value[13].String == "" {
			field.CharacterSetName = "NULL"
		} else {
			field.CharacterSetName = value[13].String
		}
		if value[14].String == "" {
			field.CollationName = "NULL"
		} else {
			field.CollationName = value[14].String
		}

		utils.SendSuccess(w, field)
	}
}
