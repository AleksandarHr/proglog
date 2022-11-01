package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// web server comprises one function - a net/http HandlerFunc(ResponseWriter, *Request)
//	for each of the API's endpoints
// Two endpoints:
//	Produce -- to write to the log
//	Consume -- to read from the log

// For an JSON/HTTP Go server, each handler consists of 3 steps:
// 	1. Unmarshal the request's JSON body into a struct
//	2. Run that endpoint's logic with the request to obtain a result
//	3. Marshal and write that result to the response
// Anything more than that should be moved into HTTP middleware and move logic down the stack

// NewHTTPServer takes in an address for the server to run on and returns an *http.Server
func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()

	// Use gorilla/mux to match incoming requests to respective handlers
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

// a server referencing a log for the server to defer to in the handlers
type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

// ProduceReqeust contains the record the user wants to append to the log
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse contains the offset the log stored the written record under
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumerRequest contains which records the API caller wants to read (e.g. the offset)
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumerResponse contains the record at the requested offset
type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	// 1. unmarshal the request body into a ProductRequest variable
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 2. run the handler logic
	offset, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 3. marshal and write the result to the response
	res := ProduceResponse{Offset: offset}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	// 1. unmarshal the request body into a ProductRequest variable
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 2. run the handler logic
	record, err := s.Log.Read(req.Offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 3. marshal and write the result to the response
	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
