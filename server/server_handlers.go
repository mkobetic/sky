package server

import (
	"github.com/skydb/sky"
	"net/http"
)

func (s *Server) addHandlers() {
	s.ApiHandleFunc("/ping", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.pingHandler(w, req, params)
	}).Methods("GET")
	s.ApiHandleFunc("/stats", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.db.Stats()
	}).Methods("GET")
	s.ApiHandleFunc("/", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.indexHandler(w, req, params)
	}).Methods("GET")
}

// GET /ping
func (s *Server) pingHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	return map[string]interface{}{"message": "ok"}, nil
}

// GET /
func (s *Server) indexHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	return map[string]interface{}{"sky": "welcome", "version": sky.Version}, nil
}
