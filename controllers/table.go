package controllers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/jinzhu/gorm"

	"restful/model"
	"restful/repository"
	"restful/utils"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
)

//DeleteData :Delete one or more records.
//@Summary Delete one or more records.
//@Tags Table
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param db_password query string true "database engine password"
//@Param fields query array true "set fields for adding value" If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)
//@Param filter query array false "SQL-like filter to delete records." If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource." example: [alias].[table]_password_[password]_by_[key1]_and_[key2]_and_..."
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name} [delete]
func (c Controller) DeleteData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			rows        *sql.Rows
			information model.Engine
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			passwords   = r.URL.Query()["db_password"]
			fields      = r.URL.Query()["fields"]
			relateds    = r.URL.Query()["related"]
			filter      = r.URL.Query()["filter"]
			slicefields []string
			coltype     []string
			password    string
			sqlorder    string
			filterorder string
			err         error
			datas       []map[string]interface{}
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
		if err = row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
			DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
			DB, err = repo.ConnectDb(information.DBType, Source)
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(filter) > 0 {
			if strings.Contains(filter[0], " and ") {
				var slicefilter []string
				split := strings.Split(filter[0], " and ")
				for i := 0; i < len(split); i++ {
					s := split[i]
					splitequal := strings.Split(s, "=")
					splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
					j := strings.Join(splitequal, "=")
					slicefilter = append(slicefilter, j)
				}
				filterorder = strings.Join(slicefilter, " and ")
			} else if strings.Contains(filter[0], " or ") {
				var slicefilter []string
				split := strings.Split(filter[0], " or ")
				for i := 0; i < len(split); i++ {
					s := split[i]
					splitequal := strings.Split(s, "=")
					splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
					j := strings.Join(splitequal, "=")
					slicefilter = append(slicefilter, j)
				}
				filterorder = strings.Join(slicefilter, " or ")
			} else if strings.Contains(filter[0], " like ") {
				filterorder = filter[0]
			} else {
				split := strings.Split(filter[0], "=")
				split[1] = fmt.Sprintf(`'%s'`, split[1])
				filterorder = strings.Join(split, "=")
			}
		}

		if len(relateds) == 0 {
			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				for i := range slicefields {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' 
						abd TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								information.DBName, tablename, slicefields[i]))
					case "mssql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where 
						TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								information.DBName, tablename, slicefields[i]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						coltype = append(coltype, "varchar")
					} else {
						coltype = append(coltype, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
					where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`, information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
					where TABLE_NAME='%s'`, information.DBName, tablename))
				}
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var table, datatype string
					rows.Scan(&table, &datatype)
					slicefields = append(slicefields, table)
					coltype = append(coltype, datatype)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(slicefields) == 0 && len(coltype) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf("select %s from %s ",
					strings.Join(slicefields, ","), tablename)
			case "mssql":
				sqlorder = fmt.Sprintf("select %s from %s.dbo.%s ",
					strings.Join(slicefields, ","), information.DBName, tablename)
			}

			if len(filter) > 0 {
				sqlorder += fmt.Sprintf("where %s ", filterorder)
			}

			var (
				value     = make([]sql.NullString, len(slicefields))
				valuePtrs = make([]interface{}, len(slicefields))
			)
			for i := 0; i < len(slicefields); i++ {
				valuePtrs[i] = &value[i]
			}

			rows, err = repo.Rowmanydata(DB, sqlorder)
			defer rows.Close()
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for rows.Next() {
				data := make(map[string]interface{})
				rows.Scan(valuePtrs...)
				for i := range slicefields {
					data[slicefields[i]] = value[i].String
					if data[slicefields[i]] == "" {
						data[slicefields[i]] = "NULL"
					} else {
						if strings.Contains(coltype[i], "int") {
							data[slicefields[i]], err = strconv.Atoi(value[i].String)
							if err != nil {
								message.Error = err.Error()
								utils.SendError(w, http.StatusInternalServerError, message)
								return
							}
						}
					}
				}
				datas = append(datas, data)
			}
		} else {
			related := relateds[0]
			sliceofrelated := strings.Split(related, ",")
			var fieldsbylocal, typesbylocal, otherfields []string
			var keys [][]string

			for i := range sliceofrelated {
				splitbyunderline := strings.Split(sliceofrelated[i], "_by_")
				keys = append(keys, strings.Split(splitbyunderline[1], "_and_"))
			}

			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				coltype = make([]string, len(slicefields))

				for i := range slicefields {
					tableandfield := strings.Split(slicefields[i], ".")
					if tableandfield[0] == tablename {
						fieldsbylocal = append(fieldsbylocal, slicefields[i])
					} else {
						otherfields = append(otherfields, slicefields[i])
					}
				}

				for i := range keys {
					fieldsbylocal = Duplicate(fieldsbylocal, keys[i], tablename)
				}

				for i := range fieldsbylocal {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where 
						TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					case "mssql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where 
						TABLE_NAME='%s' and COLUMN_NAME='%s'`, information.DBName, tablename,
							strings.Split(fieldsbylocal[i], ".")[1]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						typesbylocal = append(typesbylocal, "varchar")
					} else {
						typesbylocal = append(typesbylocal, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
					where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`, information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
					where TABLE_NAME='%s'`, information.DBName, tablename))
				}
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var table, datatype string
					rows.Scan(&table, &datatype)
					fieldsbylocal = append(fieldsbylocal, tablename+"."+table)
					typesbylocal = append(typesbylocal, datatype)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(fieldsbylocal) == 0 && len(typesbylocal) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf("select %s from %s ", strings.Join(fieldsbylocal, ","), tablename)
			case "mssql":
				sqlorder = fmt.Sprintf("select %s from %s.dbo.%s ", strings.Join(fieldsbylocal, ","),
					information.DBName, tablename)
			}
			if len(filter) > 0 {
				sqlorder += fmt.Sprintf("where %s ", filterorder)
			}

			var (
				value     = make([]sql.NullString, len(fieldsbylocal))
				valuePtrs = make([]interface{}, len(fieldsbylocal))
			)
			for i := 0; i < len(fieldsbylocal); i++ {
				valuePtrs[i] = &value[i]
			}
			rows, err = repo.Rowmanydata(DB, sqlorder)
			defer rows.Close()
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for rows.Next() {
				var data = make(map[string]interface{})
				rows.Scan(valuePtrs...)
				for i := range fieldsbylocal {
					data[fieldsbylocal[i]] = value[i].String
					if data[fieldsbylocal[i]] == "" {
						data[fieldsbylocal[i]] = "NULL"
					} else {
						if strings.Contains(typesbylocal[i], "int") {
							data[fieldsbylocal[i]], err = strconv.Atoi(value[i].String)
							if err != nil {
								message.Error = err.Error()
								utils.SendError(w, http.StatusInternalServerError, message)
								return
							}
						}
					}
				}
				datas = append(datas, data)
			}

			for i := range sliceofrelated {
				var DBEngine *gorm.DB
				var engine model.Engine
				var order string
				var joinfield, jointype []string
				var joindatas, results []map[string]interface{}

				splitbydot := strings.Split(sliceofrelated[i], ".")
				alias := splitbydot[0]
				splitbypassword := strings.Split(splitbydot[1], "_password_")
				table := splitbypassword[0]
				splitbyunderline := strings.Split(splitbypassword[1], "_by_")
				passwordforjoin := splitbyunderline[0]
				joinkey := strings.Split(splitbyunderline[1], "_and_")

				switch strings.ToLower(Storing.DBType) {
				case "mysql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from engines where db_alias='%s'`,
							alias))
				case "mssql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
							Storing.DBName, alias))
				}
				if err = row.Scan(&engine.DBAlias, &engine.DBType, &engine.DBUsername, &engine.DBPassword,
					&engine.DBHost, &engine.DBPort, &engine.DBName, &engine.Maxidle, &engine.Maxopen); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if err = bcrypt.CompareHashAndPassword([]byte(engine.DBPassword), []byte(passwordforjoin)); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusUnauthorized, message)
					return
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				case "mssql":
					Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				}
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				if len(fields) > 0 {
					for m := range otherfields {
						if strings.Split(otherfields[m], ".")[0] == table {
							joinfield = append(joinfield, otherfields[m])
						}
					}
					joinfield = Duplicate(joinfield, joinkey, table)
					for m := range joinfield {
						var datatype string
						switch strings.ToLower(engine.DBType) {
						case "mysql":
							row = repo.RowOneData(DBEngine,
								fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns 
							where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
									engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						case "mssql":
							row = repo.RowOneData(DBEngine,
								fmt.Sprintf(`select Data_type from %s.INFORMATION_SCHEMA.columns 
							where TABLE_NAME='%s' and COLUMN_NAME='%s'`, engine.DBName, table,
									strings.Split(joinfield[m], ".")[1]))
						}
						row.Scan(&datatype)
						if datatype == "" {
							jointype = append(jointype, "varchar")
						} else {
							jointype = append(jointype, datatype)
						}
					}
				} else {
					switch strings.ToLower(engine.DBType) {
					case "mysql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from 
						INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`,
								engine.DBName, table))
					case "mssql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from 
						%s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s'`,
								engine.DBName, table))
					}
					defer rows.Close()
					if err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					for rows.Next() {
						var field, datatype string
						rows.Scan(&field, &datatype)
						joinfield = append(joinfield, table+"."+field)
						jointype = append(jointype, datatype)
					}
					if err = rows.Err(); err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					if len(joinfield) == 0 && len(jointype) == 0 {
						message.Error = "The table does not exist."
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					order = fmt.Sprintf("select %s from %s ",
						strings.Join(joinfield, ","), table)
				case "mssql":
					order = fmt.Sprintf("select %s from %s.dbo.%s ",
						strings.Join(joinfield, ","),
						engine.DBName, table)
				}

				var (
					joinvalue     = make([]sql.NullString, len(joinfield))
					joinvaluePtrs = make([]interface{}, len(joinfield))
				)
				for m := 0; m < len(joinfield); m++ {
					joinvaluePtrs[m] = &joinvalue[m]
				}
				rows, err = repo.Rowmanydata(DBEngine, order)
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var joindata = make(map[string]interface{})
					rows.Scan(joinvaluePtrs...)
					for m := range joinfield {
						joindata[joinfield[m]] = joinvalue[m].String
						if joindata[joinfield[m]] == "" {
							joindata[joinfield[m]] = "NULL"
						} else {
							if strings.Contains(jointype[m], "int") {
								joindata[joinfield[m]], err = strconv.Atoi(joinvalue[m].String)
								if err != nil {
									message.Error = err.Error()
									utils.SendError(w, http.StatusInternalServerError, message)
									return
								}
							}
						}
					}
					joindatas = append(joindatas, joindata)
				}

				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				for _, data := range datas {
					for _, joindata := range joindatas {
						if len(joinkey) == 1 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 2 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 3 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] &&
								data[tablename+"."+joinkey[2]] == joindata[table+"."+joinkey[2]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						}
					}
				}
				datas = results
			}

			if len(fields) > 0 {
				var finaldatas []map[string]interface{}
				for _, data := range datas {
					result := make(map[string]interface{})
					for key, value := range data {
						for _, field := range slicefields {
							if field == key {
								result[field] = value
							}
						}
					}
					finaldatas = append(finaldatas, result)
				}
				datas = finaldatas
			}
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`delete from %s `, tablename)
		case "mssql":
			sqlorder = fmt.Sprintf(`delete from %s.dbo.%s `, information.DBName, tablename)
		}
		if len(filter) > 0 {
			sqlorder += fmt.Sprintf("where %s ", filterorder)
		}

		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		utils.SendSuccess(w, datas)
	}
}

//UpdateData :Update (replace) one or more records.
//@Summary Update (replace) one or more records.
//@Tags Table
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param db_password query string true "database engine password"
//@Param fields query array true "set fields for adding value" If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)
//@Param filter query string false "SQL-like filter to limit the records to retrieve. If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource." example: [alias].[table]_password_[password]_by_[key1]_and_[key2]_and_..."
//@Param condition body models.Description true "condition of Updating"
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name} [put]
func (c Controller) UpdateData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			rows        *sql.Rows
			information model.Engine
			description model.Description
			repo        repository.Repository
			message     model.Error
			params      = mux.Vars(r)
			tablename   = params["table_name"]
			dbalias     = params["db_alias"]
			fields      = r.URL.Query()["fields"]
			filter      = r.URL.Query()["filter"]
			relateds    = r.URL.Query()["related"]
			passwords   = r.URL.Query()["db_password"]
			slicefields []string
			coltype     []string
			filterorder string
			password    string
			sqlorder    string
			err         error
			datas       []map[string]interface{}
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

		//decode
		json.NewDecoder(r.Body).Decode(&description)
		if description.Condition == "" {
			message.Error = "Required condition for updating."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		splitdescription := strings.Split(description.Condition, ",")
		for i := range splitdescription {
			split := strings.Split(splitdescription[i], "=")
			split[1] = fmt.Sprintf(`'%s'`, split[1])
			splitdescription[i] = strings.Join(split, "=")
		}

		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring,
				fmt.Sprintf(`select * from engines where db_alias='%s'`, dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring,
				fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
					Storing.DBName, dbalias))
		}
		if err = row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
			DB, err = repo.ConnectDb(information.DBType, Source) //connect db
			sqlorder = fmt.Sprintf(`update %s set %s `, tablename, strings.Join(splitdescription, ","))
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
			DB, err = repo.ConnectDb(information.DBType, Source)
			sqlorder = fmt.Sprintf(`update %s.dbo.%s set %s `, information.DBName, tablename,
				strings.Join(splitdescription, ","))
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(filter) > 0 {
			if strings.Contains(filter[0], " and ") {
				var slicefilter []string
				split := strings.Split(filter[0], " and ")
				for i := 0; i < len(split); i++ {
					s := split[i]
					splitequal := strings.Split(s, "=")
					splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
					j := strings.Join(splitequal, "=")
					slicefilter = append(slicefilter, j)
				}
				filterorder = strings.Join(slicefilter, " and ")
			} else if strings.Contains(filter[0], " or ") {
				var slicefilter []string
				split := strings.Split(filter[0], " or ")
				for i := 0; i < len(split); i++ {
					s := split[i]
					splitequal := strings.Split(s, "=")
					splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
					j := strings.Join(splitequal, "=")
					slicefilter = append(slicefilter, j)
				}
				filterorder = strings.Join(slicefilter, " or ")
			} else if strings.Contains(filter[0], " like ") {
				filterorder = filter[0]
			} else {
				split := strings.Split(filter[0], "=")
				split[1] = fmt.Sprintf(`'%s'`, split[1])
				filterorder = strings.Join(split, "=")
			}
			sqlorder += fmt.Sprintf("where %s ", filterorder)
		}

		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(relateds) == 0 {
			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				for i := range slicefields {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' 
						abd TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								information.DBName, tablename, slicefields[i]))
					case "mssql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where 
						TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								information.DBName, tablename, slicefields[i]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						coltype = append(coltype, "varchar")
					} else {
						coltype = append(coltype, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
					where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`, information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
					where TABLE_NAME='%s'`, information.DBName, tablename))
				}
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var table, datatype string
					rows.Scan(&table, &datatype)
					slicefields = append(slicefields, table)
					coltype = append(coltype, datatype)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(slicefields) == 0 && len(coltype) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf("select %s from %s ",
					strings.Join(slicefields, ","), tablename)
			case "mssql":
				sqlorder = fmt.Sprintf("select %s from %s.dbo.%s ",
					strings.Join(slicefields, ","), information.DBName,
					tablename)
			}

			if len(filter) > 0 {
				sqlorder += fmt.Sprintf("where %s ", filterorder)
			}

			var (
				value     = make([]sql.NullString, len(slicefields))
				valuePtrs = make([]interface{}, len(slicefields))
			)
			for i := 0; i < len(slicefields); i++ {
				valuePtrs[i] = &value[i]
			}

			rows, err = repo.Rowmanydata(DB, sqlorder)
			defer rows.Close()
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for rows.Next() {
				var data = make(map[string]interface{})
				rows.Scan(valuePtrs...)
				for i := range slicefields {
					data[slicefields[i]] = value[i].String
					if data[slicefields[i]] == "" {
						data[slicefields[i]] = "NULL"
					} else {
						if strings.Contains(coltype[i], "int") {
							data[slicefields[i]], err = strconv.Atoi(value[i].String)
							if err != nil {
								message.Error = err.Error()
								utils.SendError(w, http.StatusInternalServerError, message)
								return
							}
						}
					}
				}
				datas = append(datas, data)
			}
		} else {
			related := relateds[0]
			sliceofrelated := strings.Split(related, ",")
			var fieldsbylocal, typesbylocal, otherfields []string
			var keys [][]string

			for i := range sliceofrelated {
				splitbyunderline := strings.Split(sliceofrelated[i], "_by_")
				keys = append(keys, strings.Split(splitbyunderline[1], "_and_"))
			}

			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				coltype = make([]string, len(slicefields))

				for i := range slicefields {
					tableandfield := strings.Split(slicefields[i], ".")
					if tableandfield[0] == tablename {
						fieldsbylocal = append(fieldsbylocal, slicefields[i])
					} else {
						otherfields = append(otherfields, slicefields[i])
					}
				}

				for i := range keys {
					fieldsbylocal = Duplicate(fieldsbylocal, keys[i], tablename)
				}

				for i := range fieldsbylocal {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where 
						TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					case "mssql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where 
						TABLE_NAME='%s' and COLUMN_NAME='%s'`, information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						typesbylocal = append(typesbylocal, "varchar")
					} else {
						typesbylocal = append(typesbylocal, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
					where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`, information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
					where TABLE_NAME='%s'`, information.DBName, tablename))
				}
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var table, datatype string
					rows.Scan(&table, &datatype)
					fieldsbylocal = append(fieldsbylocal, tablename+"."+table)
					typesbylocal = append(typesbylocal, datatype)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(fieldsbylocal) == 0 && len(typesbylocal) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf("select %s from %s ", strings.Join(fieldsbylocal, ","), tablename)
			case "mssql":
				sqlorder = fmt.Sprintf("select %s from %s.dbo.%s ", strings.Join(fieldsbylocal, ","),
					information.DBName, tablename)
			}

			if len(filter) > 0 {
				sqlorder += fmt.Sprintf("where %s ", filterorder)
			}

			var (
				value     = make([]sql.NullString, len(fieldsbylocal))
				valuePtrs = make([]interface{}, len(fieldsbylocal))
			)
			for i := 0; i < len(fieldsbylocal); i++ {
				valuePtrs[i] = &value[i]
			}
			rows, err = repo.Rowmanydata(DB, sqlorder)
			defer rows.Close()
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for rows.Next() {
				var data = make(map[string]interface{})
				rows.Scan(valuePtrs...)
				for i := range fieldsbylocal {
					data[fieldsbylocal[i]] = value[i].String
					if data[fieldsbylocal[i]] == "" {
						data[fieldsbylocal[i]] = "NULL"
					} else {
						if strings.Contains(typesbylocal[i], "int") {
							data[fieldsbylocal[i]], err = strconv.Atoi(value[i].String)
							if err != nil {
								message.Error = err.Error()
								utils.SendError(w, http.StatusInternalServerError, message)
								return
							}
						}
					}
				}
				datas = append(datas, data)
			}

			for i := range sliceofrelated {
				var DBEngine *gorm.DB
				var engine model.Engine
				var order string
				var joinfield, jointype []string
				var joindatas, results []map[string]interface{}

				splitbydot := strings.Split(sliceofrelated[i], ".")
				alias := splitbydot[0]
				splitbypassword := strings.Split(splitbydot[1], "_password_")
				table := splitbypassword[0]
				splitbyunderline := strings.Split(splitbypassword[1], "_by_")
				passwordforjoin := splitbyunderline[0]
				joinkey := strings.Split(splitbyunderline[1], "_and_")

				switch strings.ToLower(Storing.DBType) {
				case "mysql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from engines where db_alias='%s'`,
							alias))
				case "mssql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
							Storing.DBName, alias))
				}
				if err = row.Scan(&engine.DBAlias, &engine.DBType, &engine.DBUsername, &engine.DBPassword,
					&engine.DBHost, &engine.DBPort, &engine.DBName, &engine.Maxidle, &engine.Maxopen); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if err = bcrypt.CompareHashAndPassword([]byte(engine.DBPassword), []byte(passwordforjoin)); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusUnauthorized, message)
					return
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				case "mssql":
					Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				}
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				if len(fields) > 0 {
					for m := range otherfields {
						if strings.Split(otherfields[m], ".")[0] == table {
							joinfield = append(joinfield, otherfields[m])
						}
					}
					joinfield = Duplicate(joinfield, joinkey, table)
					for m := range joinfield {
						var datatype string
						switch strings.ToLower(engine.DBType) {
						case "mysql":
							row = repo.RowOneData(DBEngine,
								fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns 
							where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
									engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						case "mssql":
							row = repo.RowOneData(DBEngine,
								fmt.Sprintf(`select Data_type from %s.INFORMATION_SCHEMA.columns 
							where TABLE_NAME='%s' and COLUMN_NAME='%s'`, engine.DBName, table,
									strings.Split(joinfield[m], ".")[1]))
						}
						row.Scan(&datatype)
						if datatype == "" {
							jointype = append(jointype, "varchar")
						} else {
							jointype = append(jointype, datatype)
						}
					}
				} else {
					switch strings.ToLower(engine.DBType) {
					case "mysql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from 
						INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`,
								engine.DBName, table))
					case "mssql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from 
						%s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s'`,
								engine.DBName, table))
					}
					defer rows.Close()
					if err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					for rows.Next() {
						var field, datatype string
						rows.Scan(&field, &datatype)
						joinfield = append(joinfield, table+"."+field)
						jointype = append(jointype, datatype)
					}
					if err = rows.Err(); err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					if len(joinfield) == 0 && len(jointype) == 0 {
						message.Error = "The table does not exist."
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					order = fmt.Sprintf("select %s from %s ",
						strings.Join(joinfield, ","), table)
				case "mssql":
					order = fmt.Sprintf("select %s from %s.dbo.%s ",
						strings.Join(joinfield, ","),
						engine.DBName, table)
				}

				var (
					joinvalue     = make([]sql.NullString, len(joinfield))
					joinvaluePtrs = make([]interface{}, len(joinfield))
				)
				for m := 0; m < len(joinfield); m++ {
					joinvaluePtrs[m] = &joinvalue[m]
				}
				rows, err = repo.Rowmanydata(DBEngine, order)
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var joindata = make(map[string]interface{})
					rows.Scan(joinvaluePtrs...)
					for m := range joinfield {
						joindata[joinfield[m]] = joinvalue[m].String
						if joindata[joinfield[m]] == "" {
							joindata[joinfield[m]] = "NULL"
						} else {
							if strings.Contains(jointype[m], "int") {
								joindata[joinfield[m]], err = strconv.Atoi(joinvalue[m].String)
								if err != nil {
									message.Error = err.Error()
									utils.SendError(w, http.StatusInternalServerError, message)
									return
								}
							}
						}
					}
					joindatas = append(joindatas, joindata)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				for _, data := range datas {
					for _, joindata := range joindatas {
						if len(joinkey) == 1 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 2 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 3 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] &&
								data[tablename+"."+joinkey[2]] == joindata[table+"."+joinkey[2]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						}
					}
				}
				datas = results
			}

			if len(fields) > 0 {
				var finaldatas []map[string]interface{}
				for _, data := range datas {
					result := make(map[string]interface{})
					for key, value := range data {
						for _, field := range slicefields {
							if field == key {
								result[field] = value
							}
						}
					}
					finaldatas = append(finaldatas, result)
				}
				datas = finaldatas
			}
		}
		utils.SendSuccess(w, datas)
	}
}

//AddData :Create one or more records.
//@Summary Create one or more records.
//@Tags Table
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param db_password query string true "database engine password"
//@Param fields query array true "set fields for adding value" If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource." example: [alias].[table]_password_[password]_by_[key1]_and_[key2]_and_..."
//@Param value body models.InsertValue true "Insert value for adding"
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name} [post]
func (c Controller) AddData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			rows        *sql.Rows
			information model.Engine
			insertvalue model.InsertValue
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			tablename   = params["table_name"]
			dbalias     = params["db_alias"]
			fields      = r.URL.Query()["fields"]
			relateds    = r.URL.Query()["related"]
			passwords   = r.URL.Query()["db_password"]
			slicefields []string
			coltype     []string
			password    string
			sqlorder    string
			err         error
			data        = make(map[string]interface{})
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

		//decode
		json.NewDecoder(r.Body).Decode(&insertvalue)
		if insertvalue.Field == "" && insertvalue.Value == "" {
			message.Error = "Required set field and value for adding"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if len(strings.Split(insertvalue.Field, ",")) != len(strings.Split(insertvalue.Value, ",")) {
			message.Error = "Column count doesn't match value."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`, dbalias))
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
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb(information.DBType, Source)
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		splitvalue := strings.Split(insertvalue.Value, ",")
		for i := range splitvalue {
			splitvalue[i] = fmt.Sprintf(`'%s'`, splitvalue[i])
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`insert into %s(%s) values (%s)`, tablename, insertvalue.Field, strings.Join(splitvalue, ","))
		case "mssql":
			sqlorder = fmt.Sprintf(`insert into %s.dbo.%s(%s) values (%s)`, information.DBName, tablename, insertvalue.Field, strings.Join(splitvalue, ","))
		}

		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(relateds) == 0 {
			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				for i := range slicefields {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where
						TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								information.DBName, tablename, slicefields[i]))
					case "mssql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where
						TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								information.DBName, tablename, slicefields[i]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						coltype = append(coltype, "varchar")
					} else {
						coltype = append(coltype, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
				where TABLE_SCHEMA='%s' and TABLE_NAME='%s' `,
							information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
				where TABLE_NAME='%s' `,
							information.DBName, tablename))
				}
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var table, datatype string
					rows.Scan(&table, &datatype)
					slicefields = append(slicefields, table)
					coltype = append(coltype, datatype)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(slicefields) == 0 && len(coltype) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf("select %s from %s where ",
					strings.Join(slicefields, ","), tablename)
			case "mssql":
				sqlorder = fmt.Sprintf("select %s from %s.dbo.%s where ",
					strings.Join(slicefields, ","), information.DBName, tablename)
			}

			field := strings.Split(insertvalue.Field, ",")
			for i := 0; i < len(field); i++ {
				if i == len(field)-1 {
					sqlorder += fmt.Sprintf(`%s=%s`, field[i], splitvalue[i])
				} else {
					sqlorder += fmt.Sprintf(`%s=%s and `, field[i], splitvalue[i])
				}
			}

			var (
				value     = make([]sql.NullString, len(slicefields))
				valuePtrs = make([]interface{}, len(slicefields))
			)
			for i := 0; i < len(slicefields); i++ {
				valuePtrs[i] = &value[i]
			}

			row = repo.RowOneData(DB, sqlorder)
			if err = row.Scan(valuePtrs...); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for i := range slicefields {
				data[slicefields[i]] = value[i].String
				if data[slicefields[i]] == "" {
					data[slicefields[i]] = "NULL"
				} else {
					if strings.Contains(coltype[i], "int") {
						data[slicefields[i]], err = strconv.Atoi(value[i].String)
						if err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
					}
				}
			}
			utils.SendSuccess(w, data)
		} else {
			related := relateds[0]
			sliceofrelated := strings.Split(related, ",")
			var fieldsbylocal, typesbylocal, otherfields []string
			var keys [][]string
			var datas []map[string]interface{}

			for i := range sliceofrelated {
				splitbyunderline := strings.Split(sliceofrelated[i], "_by_")
				keys = append(keys, strings.Split(splitbyunderline[1], "_and_"))
			}

			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				coltype = make([]string, len(slicefields))

				for i := range slicefields {
					tableandfield := strings.Split(slicefields[i], ".")
					if tableandfield[0] == tablename {
						fieldsbylocal = append(fieldsbylocal, slicefields[i])
					} else {
						otherfields = append(otherfields, slicefields[i])
					}
				}

				for i := range keys {
					fieldsbylocal = Duplicate(fieldsbylocal, keys[i], tablename)
				}

				for i := range fieldsbylocal {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from
						 INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and 
						 TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					case "mssql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from 
						%s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						typesbylocal = append(typesbylocal, "varchar")
					} else {
						typesbylocal = append(typesbylocal, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from
					 INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`,
						information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from 
					%s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s'`, information.DBName, tablename))
				}
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var table, datatype string
					rows.Scan(&table, &datatype)
					fieldsbylocal = append(fieldsbylocal, tablename+"."+table)
					typesbylocal = append(typesbylocal, datatype)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(fieldsbylocal) == 0 && len(typesbylocal) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select %s from %s where `, strings.Join(fieldsbylocal, ","),
					tablename)
			case "mssql":
				sqlorder = fmt.Sprintf(`select %s from %s.dbo.%s where `, strings.Join(fieldsbylocal, ","),
					information.DBName, tablename)
			}

			field := strings.Split(insertvalue.Field, ",")
			for i := 0; i < len(field); i++ {
				if i == len(field)-1 {
					sqlorder += fmt.Sprintf(`%s=%s`, field[i], splitvalue[i])
				} else {
					sqlorder += fmt.Sprintf(`%s=%s and `, field[i], splitvalue[i])
				}
			}

			var (
				value     = make([]sql.NullString, len(fieldsbylocal))
				valuePtrs = make([]interface{}, len(fieldsbylocal))
			)
			for i := 0; i < len(fieldsbylocal); i++ {
				valuePtrs[i] = &value[i]
			}
			row = repo.RowOneData(DB, sqlorder)
			if err = row.Scan(valuePtrs...); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}

			for i := range fieldsbylocal {
				data[fieldsbylocal[i]] = value[i].String
				if data[fieldsbylocal[i]] == "" {
					data[fieldsbylocal[i]] = "NULL"
				} else {
					if strings.Contains(typesbylocal[i], "int") {
						data[fieldsbylocal[i]], err = strconv.Atoi(value[i].String)
						if err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
					}
				}
			}
			datas = append(datas, data)

			for i := range sliceofrelated {
				var DBEngine *gorm.DB
				var engine model.Engine
				var order string
				var joinfield, jointype []string
				var joindatas, results []map[string]interface{}

				splitbydot := strings.Split(sliceofrelated[i], ".")
				alias := splitbydot[0]
				splitbypassword := strings.Split(splitbydot[1], "_password_")
				table := splitbypassword[0]
				splitbyunderline := strings.Split(splitbypassword[1], "_by_")
				passwordforjoin := splitbyunderline[0]
				joinkey := strings.Split(splitbyunderline[1], "_and_")

				switch strings.ToLower(Storing.DBType) {
				case "mysql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from engines where db_alias='%s'`, alias))
				case "mssql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
							Storing.DBName, alias))
				}
				if err = row.Scan(&engine.DBAlias, &engine.DBType, &engine.DBUsername,
					&engine.DBPassword, &engine.DBHost, &engine.DBPort,
					&engine.DBName, &engine.Maxidle, &engine.Maxopen); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if err = bcrypt.CompareHashAndPassword([]byte(engine.DBPassword), []byte(passwordforjoin)); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusUnauthorized, message)
					return
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				case "mssql":
					Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				}
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				if len(fields) > 0 {
					for m := range otherfields {
						if strings.Split(otherfields[m], ".")[0] == table {
							joinfield = append(joinfield, otherfields[m])
						}
					}
					joinfield = Duplicate(joinfield, joinkey, table)
					for m := range joinfield {
						var datatype string
						switch strings.ToLower(engine.DBType) {
						case "mysql":
							row = repo.RowOneData(DBEngine, fmt.Sprintf(`select Data_Type from 
							INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and
							 COLUMN_NAME='%s'`, engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						case "mssql":
							row = repo.RowOneData(DBEngine, fmt.Sprintf(`select Data_Type from 
							%s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						}
						row.Scan(&datatype)
						if datatype == "" {
							jointype = append(jointype, "varchar")
						} else {
							jointype = append(jointype, datatype)
						}
					}
				} else {
					switch strings.ToLower(engine.DBType) {
					case "mysql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
						where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`, engine.DBName, table))
					case "mssql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s'`, engine.DBName, table))
					}
					defer rows.Close()
					if err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					for rows.Next() {
						var field, datatype string
						rows.Scan(&field, &datatype)
						joinfield = append(joinfield, table+"."+field)
						jointype = append(jointype, datatype)
					}
					if err = rows.Err(); err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					if len(joinfield) == 0 && len(jointype) == 0 {
						message.Error = "The table does not exist."
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					order = fmt.Sprintf(`select %s from %s `, strings.Join(joinfield, ","), table)
				case "mssql":
					order = fmt.Sprintf(`select %s from %s.dbo.%s`,
						strings.Join(joinfield, ","), engine.DBName, table)
				}

				var (
					joinvalue     = make([]sql.NullString, len(joinfield))
					joinvaluePtrs = make([]interface{}, len(joinfield))
				)
				for m := 0; m < len(joinfield); m++ {
					joinvaluePtrs[m] = &joinvalue[m]
				}
				rows, err = repo.Rowmanydata(DBEngine, order)
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var joindata = make(map[string]interface{})
					rows.Scan(joinvaluePtrs...)
					for m := range joinfield {
						joindata[joinfield[m]] = joinvalue[m].String
						if joindata[joinfield[m]] == "" {
							joindata[joinfield[m]] = "NULL"
						} else {
							if strings.Contains(jointype[m], "int") {
								joindata[joinfield[m]], err = strconv.Atoi(joinvalue[m].String)
								if err != nil {
									message.Error = err.Error()
									utils.SendError(w, http.StatusInternalServerError, message)
									return
								}
							}
						}
					}
					joindatas = append(joindatas, joindata)
				}

				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				for _, data := range datas {
					for _, joindata := range joindatas {
						if len(joinkey) == 1 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 2 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 3 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] &&
								data[tablename+"."+joinkey[2]] == joindata[table+"."+joinkey[2]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						}
					}
				}
				datas = results
			}

			if len(fields) > 0 {
				var finaldatas []map[string]interface{}
				for _, data := range datas {
					result := make(map[string]interface{})
					for key, value := range data {
						for _, field := range slicefields {
							if field == key {
								result[field] = value
							}
						}
					}
					finaldatas = append(finaldatas, result)
				}
				datas = finaldatas
			}
			utils.SendSuccess(w, datas)
		}
	}
}

//GetAllData :Retrieve one or more records.
//@Summary Retrieve one or more records.
//@Tags Table
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param db_password query string true "database engine password"
//@Param fields query array false "Comma-delimited list of properties to be returned for each resource, "*" returns all properties. If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)"
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource. example: [alias].[table]_password_[password]_by_[key1]_and_[key2]_and_..."
//@Param filter query string false "SQL-like filter to limit the records to retrieve. If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)"
//@Param limit query integer false "Set to limit the filter results."
//@Param offset query integer false "Set to offset the filter results to a particular record count."
//@Param order query string false "SQL-like order containing field and direction for filter results. If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)"
//@Param group query string false "Comma-delimited list of the fields used for grouping of filter results. If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)"
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name} [get]
func (c Controller) GetAllData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			rows        *sql.Rows
			information model.Engine
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			tablename   = params["table_name"]
			dbalias     = params["db_alias"]
			passwords   = r.URL.Query()["db_password"]
			fields      = r.URL.Query()["fields"]
			relateds    = r.URL.Query()["related"]
			filter      = r.URL.Query()["filter"]
			order       = r.URL.Query()["order"]
			limit       = r.URL.Query()["limit"]
			offset      = r.URL.Query()["offset"]
			group       = r.URL.Query()["group"]
			password    string
			related     string
			sqlorder    string
			slicefields []string
			coltype     []string
			err         error
			datas       []map[string]interface{}
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
			row = repo.RowOneData(DBStoring,
				fmt.Sprintf(`select * from engines where db_alias='%s'`, dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring,
				fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`, Storing.DBName, dbalias))
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
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb(information.DBType, Source) //connect db

		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb(information.DBType, Source)
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(relateds) == 0 {
			switch strings.ToLower(information.DBType) {
			case "mysql":
				if len(fields) > 0 {
					slicefields = strings.Split(fields[0], ",")
					sqlorder = fmt.Sprintf("select %s from %s.%s ", fields[0], information.DBName, tablename)
					for i := range slicefields {
						var datatype string
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' 
							and TABLE_NAME='%s' and COLUMN_NAME='%s' `,
								information.DBName, tablename, slicefields[i]))
						row.Scan(&datatype)
						if datatype == "" {
							coltype = append(coltype, "varchar")
						} else {
							coltype = append(coltype, datatype)
						}
					}
				} else if len(fields) == 0 {
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
							 where TABLE_SCHEMA='%s' and TABLE_NAME='%s' `, information.DBName, tablename))
					defer rows.Close()
					if err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					for rows.Next() {
						var table string
						var datatype string
						rows.Scan(&table, &datatype)
						slicefields = append(slicefields, table)
						coltype = append(coltype, datatype)
					}
					if err = rows.Err(); err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					if len(slicefields) == 0 && len(coltype) == 0 {
						message.Error = "The table does not exist."
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					sqlorder = fmt.Sprintf("select %s from %s.%s ",
						strings.Join(slicefields, ","), information.DBName, tablename)
				}

				if len(filter) > 0 {
					if strings.Contains(filter[0], " and ") {
						var slicefilter []string
						split := strings.Split(filter[0], " and ")
						for i := 0; i < len(split); i++ {
							s := split[i]
							splitequal := strings.Split(s, "=")
							splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
							j := strings.Join(splitequal, "=")
							slicefilter = append(slicefilter, j)
						}
						j := strings.Join(slicefilter, " and ")
						sqlorder += fmt.Sprintf("where %s ", j)
					} else if strings.Contains(filter[0], " or ") {
						var slicefilter []string
						split := strings.Split(filter[0], " or ")
						for i := 0; i < len(split); i++ {
							s := split[i]
							splitequal := strings.Split(s, "=")
							splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
							j := strings.Join(splitequal, "=")
							slicefilter = append(slicefilter, j)
						}
						j := strings.Join(split, " or ")
						sqlorder += fmt.Sprintf("where %s", j)
					} else if strings.Contains(filter[0], "like") {
						sqlorder += fmt.Sprintf("where %s ", filter[0])
					} else {
						split := strings.Split(filter[0], "=")
						split[1] = fmt.Sprintf(`'%s'`, split[1])
						j := strings.Join(split, "=")
						sqlorder += fmt.Sprintf("where %s ", j)
					}
				}
				if len(group) > 0 {
					sqlorder += fmt.Sprintf("group by %s ", group[0])
				}
				if len(order) > 0 {
					sqlorder += fmt.Sprintf("order by %s ", order[0])
				}
				if len(limit) > 0 {
					sqlorder += fmt.Sprintf("limit %s ", limit[0])
				} else {
					sqlorder += fmt.Sprintf("limit 1000 ")
				}
				if len(offset) > 0 {
					sqlorder += fmt.Sprintf("offset %s ", offset[0])
				} else {
					sqlorder += fmt.Sprintf("offset 0 ")
				}

			case "mssql":
				if len(offset) == 0 {
					if len(limit) > 0 {
						sqlorder = fmt.Sprintf(`select top %s `, limit[0])
					} else {
						sqlorder = fmt.Sprintf(`select top 1000 `)
					}

					if len(fields) > 0 {
						slicefields = strings.Split(fields[0], ",")
						sqlorder += fmt.Sprintf("%s from %s.dbo.%s ", fields[0], information.DBName, tablename)
						for i := range slicefields {
							var datatype string
							row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
							row.Scan(&datatype)
							if datatype == "" {
								coltype = append(coltype, "varchar")
							} else {
								coltype = append(coltype, datatype)
							}
						}
					} else if len(fields) == 0 {
						rows, err = repo.Rowmanydata(DB,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns
										 where TABLE_NAME='%s' `, information.DBName, tablename))
						defer rows.Close()
						if err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
						for rows.Next() {
							var table string
							var datatype string
							rows.Scan(&table, &datatype)
							slicefields = append(slicefields, table)
							coltype = append(coltype, datatype)
						}
						if err = rows.Err(); err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
						if len(slicefields) == 0 && len(coltype) == 0 {
							message.Error = "The table does not exist."
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
						sqlorder += fmt.Sprintf("%s from %s.dbo.%s ", strings.Join(slicefields, ","), information.DBName, tablename)
					}
					if len(filter) > 0 {
						if strings.Contains(filter[0], " and ") {
							var slicefilter []string
							split := strings.Split(filter[0], " and ")
							for i := 0; i < len(split); i++ {
								s := split[i]
								splitequal := strings.Split(s, "=")
								splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
								j := strings.Join(splitequal, "=")
								slicefilter = append(slicefilter, j)
							}
							j := strings.Join(slicefilter, " and ")
							sqlorder += fmt.Sprintf("where %s ", j)
						} else if strings.Contains(filter[0], " or ") {
							var slicefilter []string
							split := strings.Split(filter[0], " or ")
							for i := 0; i < len(split); i++ {
								s := split[i]
								splitequal := strings.Split(s, "=")
								splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
								j := strings.Join(splitequal, "=")
								slicefilter = append(slicefilter, j)
							}
							j := strings.Join(slicefilter, " or ")
							sqlorder += fmt.Sprintf("where %s", j)
						} else if strings.Contains(filter[0], "like") {
							sqlorder += fmt.Sprintf("where %s ", filter[0])
						} else {
							split := strings.Split(filter[0], "=")
							split[1] = fmt.Sprintf(`'%s'`, split[1])
							j := strings.Join(split, "=")
							sqlorder += fmt.Sprintf("where %s ", j)
						}
					}

					if len(group) > 0 {
						sqlorder += fmt.Sprintf("group by %s ", group[0])
					}
					if len(order) > 0 {
						sqlorder += fmt.Sprintf("order by %s ", order[0])
					} else {
						sqlorder += fmt.Sprintf("order by (select 0)")
					}
				} else {
					if len(fields) > 0 {
						slicefields = strings.Split(fields[0], ",")
						sqlorder = fmt.Sprintf("select * from(select %s, row_number() over ", fields[0])
						for i := range slicefields {
							var datatype string
							row = repo.RowOneData(DB,
								fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s' `,
									information.DBName, tablename, slicefields[i]))
							if datatype == "" {
								coltype = append(coltype, "varchar")
							} else {
								coltype = append(coltype, datatype)
							}
						}
						slicefields = append(slicefields, "row_num")
						coltype = append(coltype, "int")
					} else if len(fields) == 0 {
						rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' `,
							information.DBName, tablename))
						defer rows.Close()
						if err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
						for rows.Next() {
							var table string
							var datatype string
							rows.Scan(&table, &datatype)
							slicefields = append(slicefields, table)
							coltype = append(coltype, datatype)
						}
						if err = rows.Err(); err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
						if len(slicefields) == 0 && len(coltype) == 0 {
							message.Error = "The table does not exist."
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
						sqlorder = fmt.Sprintf("select * from(select %s, row_number() over ", strings.Join(slicefields, ","))
						slicefields = append(slicefields, "row_num")
						coltype = append(coltype, "int")
					}

					if len(order) > 0 {
						sqlorder += fmt.Sprintf("(order by %s) as row_num from %s.dbo.%s ", order[0], information.DBName, tablename)
					} else {
						sqlorder += fmt.Sprintf("(order by (select 0)) as row_num from %s.dbo.%s ", information.DBName, tablename)
					}

					if len(filter) > 0 {
						if strings.Contains(filter[0], " and ") {
							var slicefilter []string
							split := strings.Split(filter[0], " and ")
							for i := 0; i < len(split); i++ {
								s := split[i]
								splitequal := strings.Split(s, "=")
								splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
								j := strings.Join(splitequal, "=")
								slicefilter = append(slicefilter, j)
							}
							j := strings.Join(slicefilter, " and ")
							sqlorder += fmt.Sprintf("where %s ", j)
						} else if strings.Contains(filter[0], " or ") {
							var slicefilter []string
							split := strings.Split(filter[0], " or ")
							for i := 0; i < len(split); i++ {
								s := split[i]
								splitequal := strings.Split(s, "=")
								splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
								j := strings.Join(splitequal, "=")
								slicefilter = append(slicefilter, j)
							}
							j := strings.Join(slicefilter, " or ")
							sqlorder += fmt.Sprintf("where %s", j)
						} else if strings.Contains(filter[0], "like") {
							sqlorder += fmt.Sprintf("where %s ", filter[0])
						} else {
							split := strings.Split(filter[0], "=")
							split[1] = fmt.Sprintf(`'%s'`, split[1])
							j := strings.Join(split, "=")
							sqlorder += fmt.Sprintf("where %s ", j)
						}
					}

					o, err := strconv.Atoi(offset[0])
					if err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					if len(group) > 0 {
						sqlorder += fmt.Sprintf("group by %s) as temp_table where row_num between %d and ", group[0], o+1)
					} else {
						sqlorder += fmt.Sprintf(") as temp_table where row_num between %d and ", o+1)
					}
					if len(limit) > 0 {
						l, err := strconv.Atoi(limit[0])
						if err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
						sqlorder += fmt.Sprintf("%d", o+l)
					} else {
						sqlorder += fmt.Sprintf("%d", o+1000)
					}
				}
			}
			var (
				value     = make([]sql.NullString, len(slicefields))
				valuePtrs = make([]interface{}, len(slicefields)) //scan need pointer
			)
			for i := 0; i < len(slicefields); i++ {
				valuePtrs[i] = &value[i]
			}

			rows, err = repo.Rowmanydata(DB, sqlorder)
			defer rows.Close()
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for rows.Next() {
				var data = make(map[string]interface{})
				rows.Scan(valuePtrs...)
				for i := range slicefields {
					data[slicefields[i]] = value[i].String
					if data[slicefields[i]] == "" {
						data[slicefields[i]] = "NULL"
					} else {
						if strings.Contains(coltype[i], "int") {
							data[slicefields[i]], err = strconv.Atoi(value[i].String)
							if err != nil {
								message.Error = err.Error()
								utils.SendError(w, http.StatusInternalServerError, message)
								return
							}
						}
					}
				}
				datas = append(datas, data)
			}
			if err = rows.Err(); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
		} else {
			related = relateds[0]
			sliceofrelated := strings.Split(related, ",")
			var fieldsbylocal, typesbylocal []string
			var otherfields, otherfilters, othergroups, otherorders []string
			var keys [][]string

			for i := range sliceofrelated {
				splitbyunderline := strings.Split(sliceofrelated[i], "_by_")
				keys = append(keys, strings.Split(splitbyunderline[1], "_and_"))
			}

			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				coltype = make([]string, len(slicefields))
				for i := range slicefields {
					tableandfield := strings.Split(slicefields[i], ".")
					if tableandfield[0] == tablename {
						fieldsbylocal = append(fieldsbylocal, slicefields[i])
					} else {
						otherfields = append(otherfields, slicefields[i])
					}
				}
				for i := range keys {
					fieldsbylocal = Duplicate(fieldsbylocal, keys[i], tablename)
				}

				for i := range fieldsbylocal {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns 
						where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					case "mssql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						typesbylocal = append(typesbylocal, "varchar")
					} else {
						typesbylocal = append(typesbylocal, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
						where TABLE_SCHEMA='%s' and TABLE_NAME='%s' `, information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s' `, information.DBName, tablename))
				}
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var table, datatype string
					rows.Scan(&table, &datatype)
					fieldsbylocal = append(fieldsbylocal, tablename+"."+table)
					typesbylocal = append(typesbylocal, datatype)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(fieldsbylocal) == 0 && len(typesbylocal) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select %s from %s.%s `, strings.Join(fieldsbylocal, ","),
					information.DBName, tablename)
			case "mssql":
				sqlorder = fmt.Sprintf(`select %s from %s.dbo.%s `, strings.Join(fieldsbylocal, ","),
					information.DBName, tablename)
			}

			if len(filter) > 0 {
				if strings.Contains(filter[0], " and ") {
					var localfilter []string
					split := strings.Split(filter[0], " and ")
					for i := range split {
						s := split[i]
						splitequal := strings.Split(s, "=")
						splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
						j := strings.Join(splitequal, "=")
						splitdot := strings.Split(s, ".")
						if splitdot[0] == tablename {
							localfilter = append(localfilter, j)
						} else {
							otherfilters = append(otherfilters, j)
						}
					}
					j := strings.Join(localfilter, " and ")
					if len(j) > 0 {
						sqlorder += fmt.Sprintf("where %s ", j)
					}
				} else if strings.Contains(filter[0], " or ") {
					var localfilter []string
					split := strings.Split(filter[0], " or ")
					for i := range split {
						s := split[i]
						splitequal := strings.Split(s, "=")
						splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
						j := strings.Join(splitequal, "=")
						splitdot := strings.Split(s, ".")
						if splitdot[0] == tablename {
							localfilter = append(localfilter, j)
						} else {
							otherfilters = append(otherfilters, j)
						}
					}
					j := strings.Join(localfilter, " or ")
					if len(j) > 0 {
						sqlorder += fmt.Sprintf("where %s ", j)
					}
				} else if strings.Contains(filter[0], " like ") {
					split := strings.Split(filter[0], " like ")
					if strings.Split(split[0], ".")[0] == tablename {
						sqlorder += fmt.Sprintf("where %s ", filter[0])
					} else {
						otherfilters = append(otherfilters, filter[0])
					}
				} else {
					split := strings.Split(filter[0], "=")
					split[1] = fmt.Sprintf(`'%s'`, split[1])
					j := strings.Join(split, "=")
					if strings.Split(filter[0], ".")[0] == tablename {
						sqlorder += fmt.Sprintf("where %s ", j)
					} else {
						otherfilters = append(otherfilters, j)
					}
				}
			}

			if len(group) > 0 {
				var localgroup []string
				groups := strings.Split(group[0], ",")
				for i := range groups {
					if strings.Split(groups[i], ".")[0] == tablename {
						localgroup = append(localgroup, groups[i])
					} else {
						othergroups = append(othergroups, groups[i])
					}
				}
				if len(localgroup) > 0 {
					sqlorder += fmt.Sprintf("group by %s ", strings.Join(localgroup, ","))
				}
			}

			if len(order) > 0 {
				var localorder []string
				orders := strings.Split(order[0], ",")
				for i := range orders {
					if strings.Split(orders[i], ".")[0] == tablename {
						localorder = append(localorder, orders[i])
					} else {
						otherorders = append(otherorders, orders[i])
					}
				}
				if len(localorder) > 0 {
					sqlorder += fmt.Sprintf("order by %s ", strings.Join(localorder, ","))
				}
			}

			var (
				value     = make([]sql.NullString, len(fieldsbylocal))
				valuePtrs = make([]interface{}, len(fieldsbylocal))
			)
			for i := 0; i < len(fieldsbylocal); i++ {
				valuePtrs[i] = &value[i]
			}
			rows, err = repo.Rowmanydata(DB, sqlorder)
			defer rows.Close()
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for rows.Next() {
				var data = make(map[string]interface{})
				rows.Scan(valuePtrs...)
				for i := range fieldsbylocal {
					data[fieldsbylocal[i]] = value[i].String
					if data[fieldsbylocal[i]] == "" {
						data[fieldsbylocal[i]] = "NULL"
					} else {
						if strings.Contains(typesbylocal[i], "int") {
							data[fieldsbylocal[i]], err = strconv.Atoi(value[i].String)
							if err != nil {
								message.Error = err.Error()
								utils.SendError(w, http.StatusInternalServerError, message)
								return
							}
						}
					}
				}
				datas = append(datas, data)
			}
			if err = rows.Err(); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}

			for i := range sliceofrelated {
				var DBEngine *gorm.DB
				var engine model.Engine
				var order string
				var joinfield, jointype, joinfilter, joingroup, joinorder []string
				var joindatas, results []map[string]interface{}
				splitbydot := strings.Split(sliceofrelated[i], ".")
				alias := splitbydot[0]
				splitbypassword := strings.Split(splitbydot[1], "_password_")
				table := splitbypassword[0]
				splitbyunderline := strings.Split(splitbypassword[1], "_by_")
				passwordforjoin := splitbyunderline[0]
				joinkey := strings.Split(splitbyunderline[1], "_and_")

				switch strings.ToLower(Storing.DBType) {
				case "mysql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from engines where db_alias='%s'`, alias))
				case "mssql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
							Storing.DBName, alias))
				}
				if err = row.Scan(&engine.DBAlias, &engine.DBType, &engine.DBUsername,
					&engine.DBPassword, &engine.DBHost, &engine.DBPort,
					&engine.DBName, &engine.Maxidle, &engine.Maxopen); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if err = bcrypt.CompareHashAndPassword([]byte(engine.DBPassword), []byte(passwordforjoin)); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusUnauthorized, message)
					return
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
						engine.DBUsername,
						passwordforjoin,
						engine.DBHost,
						engine.DBPort,
						engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source) //connect db

				case "mssql":
					Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
						engine.DBUsername,
						passwordforjoin,
						engine.DBHost,
						engine.DBPort,
						engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				}
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
				}

				if len(fields) > 0 {
					for m := range otherfields {
						if strings.Split(otherfields[m], ".")[0] == table {
							joinfield = append(joinfield, otherfields[m])
						}
					}
					joinfield = Duplicate(joinfield, joinkey, table)
					for m := range joinfield {
						var datatype string
						switch strings.ToLower(engine.DBType) {
						case "mysql":
							row = repo.RowOneData(DBEngine, fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns
							where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						case "mssql":
							row = repo.RowOneData(DBEngine, fmt.Sprintf(`select Data_type from
							%s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						}
						row.Scan(&datatype)
						if datatype == "" {
							jointype = append(jointype, "varchar")
						} else {
							jointype = append(jointype, datatype)
						}
					}

					switch strings.ToLower(engine.DBType) {
					case "mysql":
						order = fmt.Sprintf(`select %s from %s.%s `,
							strings.Join(joinfield, ","), engine.DBName, table)
					case "mssql":
						order = fmt.Sprintf("select %s from %s.dbo.%s ",
							strings.Join(joinfield, ","), engine.DBName, table)
					}

				} else {
					switch strings.ToLower(engine.DBType) {
					case "mysql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
						where TABLE_SCHEMA='%s' and TABLE_NAME='%s' `, engine.DBName, table))
					case "mssql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns
							where TABLE_NAME='%s' `, engine.DBName, table))
					}
					defer rows.Close()
					if err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					for rows.Next() {
						var field, datatype string
						rows.Scan(&field, &datatype)
						joinfield = append(joinfield, table+"."+field)
						jointype = append(jointype, datatype)
					}
					if err = rows.Err(); err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					if len(joinfield) == 0 && len(jointype) == 0 {
						message.Error = "The table does not exist."
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}

					switch strings.ToLower(engine.DBType) {
					case "mysql":
						order = fmt.Sprintf(`select %s from %s.%s `,
							strings.Join(joinfield, ","), engine.DBName, table)
					case "mssql":
						order = fmt.Sprintf("select %s from %s.dbo.%s ",
							strings.Join(joinfield, ","), engine.DBName, table)
					}
				}

				if len(otherfilters) > 0 {
					if strings.Contains(filter[0], " and ") {
						for m := range otherfilters {
							if strings.Split(otherfilters[m], ".")[0] == table {
								joinfilter = append(joinfilter, otherfilters[m])
							}
						}
						j := strings.Join(joinfilter, " and ")
						if len(j) > 0 {
							order += fmt.Sprintf("where %s ", j)
						}
					} else if strings.Contains(filter[0], " or ") {
						for m := range otherfilters {
							if strings.Split(otherfilters[m], ".")[0] == table {
								joinfilter = append(joinfilter, otherfilters[m])
							}
						}
						j := strings.Join(joinfilter, " or ")
						if len(j) > 0 {
							order += fmt.Sprintf("where %s ", j)
						}
					} else if strings.Contains(filter[0], " like ") {
						if strings.Split(otherfilters[0], ".")[0] == table {
							order += fmt.Sprintf("where %s ", otherfilters[0])
						}
					} else {
						if strings.Split(otherfilters[0], ".")[0] == table {
							order += fmt.Sprintf("where %s ", otherfilters[0])
						}
					}
				}

				if len(othergroups) > 0 {
					for m := range othergroups {
						if strings.Split(othergroups[m], ".")[0] == table {
							joingroup = append(joingroup, othergroups[m])
						}
					}
					if len(joingroup) > 0 {
						order += fmt.Sprintf("group by %s ", strings.Join(joingroup, ","))
					}
				}
				if len(otherorders) > 0 {
					for m := range otherorders {
						if strings.Split(otherorders[m], ".")[0] == table {
							joinorder = append(joinorder, otherorders[m])
						}
					}
					if len(joinorder) > 0 {
						order += fmt.Sprintf("order by %s ", strings.Join(joinorder, ","))
					}
				}

				var (
					joinvalue     = make([]sql.NullString, len(joinfield))
					joinvaluePtrs = make([]interface{}, len(joinfield))
				)
				for m := 0; m < len(joinfield); m++ {
					joinvaluePtrs[m] = &joinvalue[m]
				}
				rows, err = repo.Rowmanydata(DBEngine, order)
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var joindata = make(map[string]interface{})
					rows.Scan(joinvaluePtrs...)
					for m := range joinfield {
						joindata[joinfield[m]] = joinvalue[m].String
						if joindata[joinfield[m]] == "" {
							joindata[joinfield[m]] = "NULL"
						} else {
							if strings.Contains(jointype[m], "int") {
								joindata[joinfield[m]], err = strconv.Atoi(joinvalue[m].String)
								if err != nil {
									message.Error = err.Error()
									utils.SendError(w, http.StatusInternalServerError, message)
									return
								}
							}
						}
					}
					joindatas = append(joindatas, joindata)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				for _, data := range datas {
					for _, joindata := range joindatas {
						if len(joinkey) == 1 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 2 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 3 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] &&
								data[tablename+"."+joinkey[2]] == joindata[table+"."+joinkey[2]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						}
					}
				}
				datas = results
			}

			if len(fields) > 0 {
				var finaldatas []map[string]interface{}
				for _, data := range datas {
					result := make(map[string]interface{})
					for key, value := range data {
						for _, field := range slicefields {
							if field == key {
								result[field] = value
							}
						}
					}
					finaldatas = append(finaldatas, result)
				}
				datas = finaldatas
			}

			if len(offset) > 0 {
				n, err := strconv.Atoi(offset[0])
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				datas = datas[n:]
			}

			if len(limit) > 0 {
				n, err := strconv.Atoi(limit[0])
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				datas = datas[:n]
			}
		}
		utils.SendSuccess(w, datas)
	}
}

//Duplicate :CHECK
func Duplicate(a []string, b []string, name string) []string {
	check := make(map[string]int)
	c := make([]string, len(b))

	for i := range b {
		c[i] = name + "." + b[i]
	}

	d := append(a, c...)
	res := make([]string, 0)
	for _, val := range d {
		check[val] = 1
	}

	for letter := range check {
		res = append(res, letter)
	}

	return res
}
