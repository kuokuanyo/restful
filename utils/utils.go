package utils

import (
	"encoding/json"
	"net/http"
	"restful/model"
)

//SendError :response error
func SendError(w http.ResponseWriter, status int, message model.Error) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(message)
}

//SendSuccess :response success
func SendSuccess(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}
