package controllers

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"restful/model"
	"restful/repository"
	"restful/utils"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
)

//GetAllData :Retrieve one or more records.
func (c Controller) GetAllData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			message     model.Error
			params      = mux.Vars(r)
			password    = r.URL.Query()["db_password"][0]
			tablename   = params["table_name"]
			dbalias     = params["db_alias"]
			fields      = r.URL.Query()["fields"]
			filter      = r.URL.Query()["filter"]
			order       = r.URL.Query()["order"]
			limit       = r.URL.Query()["limit"]
			offset      = r.URL.Query()["offset"]
			fetch       = r.URL.Query()["fetch"]
			group       = r.URL.Query()["group"]
			information model.DBInformation
			repo        repository.Repository
			sqlorder    string
			slicefields []string
			rows        *sql.Rows
			datas       []map[string]interface{}
			coltype     []string
		)
		if password == "" {
			message.Error = "Require password"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//get informations from db_alias
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = "Connect mysql.user db error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		row := repo.RawOneData(DB, fmt.Sprintf(`select * from users where db_alias="%s"`, dbalias))
		//scan information
		row.Scan(&information.DBAlias, &information.DBType, &information.DBUserName,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.MaxIdle, &information.MaxOpen)
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = "Error password."
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
				message.Error = "Database information error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				sqlorder = fmt.Sprintf("select %s from %s ", fields[0], tablename)
				for i := range slicefields {
					var datatype string
					row = repo.RawOneData(DB, fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
					row.Scan(&datatype)
					if datatype == "" {
						coltype = append(coltype, "varchar")
					} else {
						coltype = append(coltype, datatype)
					}
				}
			} else if len(fields) == 0 {
				sqlorder = fmt.Sprintf("select * from %s ", tablename)
				rows, err = repo.Raw(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' `, information.DBName, tablename))
				for rows.Next() {
					var table string
					var datatype string
					rows.Scan(&table, &datatype)
					slicefields = append(slicefields, table)
					coltype = append(coltype, datatype)
				}
				if err != nil {
					message.Error = "scan information of table error"
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
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
				} else if strings.Contains(filter[0], "like") {
					sqlorder += fmt.Sprintf("where %s ", filter[0])
				} else {
					split := strings.Split(filter[0], "=")
					split[1] = fmt.Sprintf(`'%s'`, split[1])
					j := strings.Join(split, "=")
					sqlorder += fmt.Sprintf("where %s ", j)
				}
			}
			if len(limit) > 0 {
				sqlorder += fmt.Sprintf("limit %s ", limit[0])
			}
			if len(offset) > 0 {
				if len(limit) > 0 {
					sqlorder += fmt.Sprintf("offset %s ", offset[0])
				} else if len(limit) == 0 {
					message.Error = "the offset parameter must have the parameter of limit"
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}
			if len(order) > 0 {
				sqlorder += fmt.Sprintf("order by %s ", order[0])
			}
			if len(group) > 0 {
				sqlorder += fmt.Sprintf("group by %s ", group[0])
			}
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
			if err != nil {
				message.Error = "Database information error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			sqlorder = "select "
			if len(limit) > 0 {
				sqlorder = fmt.Sprintf("select top %s ", limit[0])
			}
			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				sqlorder += fmt.Sprintf("%s from %s.dbo.%s ", fields[0], information.DBName, tablename)
				for i := range slicefields {
					var datatype string
					row = repo.RawOneData(DB, fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
					row.Scan(&datatype)
					if datatype == "" {
						coltype = append(coltype, "varchar")
					} else {
						coltype = append(coltype, datatype)
					}
				}
			} else if len(fields) == 0 {
				sqlorder += fmt.Sprintf("* from %s.dbo.%s ", information.DBName, tablename)
				rows, err = repo.Raw(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' `, information.DBName, tablename))
				for rows.Next() {
					var table string
					var datatype string
					rows.Scan(&table, &datatype)
					slicefields = append(slicefields, table)
					coltype = append(coltype, datatype)
				}
				if err != nil {
					message.Error = "scan information of table error"
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}
			if len(order) > 0 {
				sqlorder += fmt.Sprintf("order by %s ", order[0])
			}
			if len(offset) > 0 {
				if len(order) > 0 && len(fetch) > 0 {
					sqlorder += fmt.Sprintf("offset %s rows fetch next %s rows only ", offset[0], fetch[0])
				} else if len(order) == 0 || len(fetch) == 0 {
					message.Error = "the offset parameter must have the parameter of order and fetch."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}
			if len(group) > 0 {
				sqlorder += fmt.Sprintf("group by %s ", group[0])
			}
		}
		fmt.Println(slicefields)
		fmt.Println(coltype)
		fmt.Println(sqlorder)
		var (
			value     = make([]string, len(slicefields))
			valuePtrs = make([]interface{}, len(slicefields)) //scan need pointer
		)
		for i := 0; i < len(slicefields); i++ {
			valuePtrs[i] = &value[i]
		}
		rows, err = repo.Raw(DB, sqlorder)
		if err != nil {
			message.Error = "Get datas error."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		for rows.Next() {
			var data = make(map[string]interface{})
			rows.Scan(valuePtrs...)
			for i := range slicefields {
				if strings.Contains(coltype[i], "varchar") {
					data[slicefields[i]] = value[i]
				} else if strings.Contains(coltype[i], "int") {
					data[slicefields[i]], err = strconv.Atoi(value[i])
					if err != nil {
						message.Error = "value convert error."
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
				}
			}
			datas = append(datas, data)
		}
		utils.SendSuccess(w, datas)
	}
}