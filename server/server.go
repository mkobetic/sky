package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/skydb/sky"
	"github.com/skydb/sky/core"
	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query"
)

// The number of servlets created on startup defaults to the number
// of logical cores on a machine.
var defaultServletCount = runtime.NumCPU()

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A Server is the front end that controls access to tables.
type Server struct {
	httpServer           *http.Server
	router               *mux.Router
	logger               *log.Logger
	db                   db.DB
	path                 string
	listener             net.Listener
	tables               map[string]*core.Table
	shutdownChannel      chan bool
	shutdownFinished     chan bool
	mutex                sync.Mutex
	NoSync               bool
	MaxDBs               uint
	MaxReaders           uint
	StreamFlushPeriod    uint
	StreamFlushThreshold uint
}

//------------------------------------------------------------------------------
//
// Errors
//
//------------------------------------------------------------------------------

//--------------------------------------
// Alternate Content Type
//--------------------------------------

// Hackish way to return plain text for query codegen. Might fix later but
// it's rarely used.
type TextPlainContentTypeError struct {
}

func (e *TextPlainContentTypeError) Error() string {
	return ""
}

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// NewServer returns a new Server.
func NewServer(port uint, path string) *Server {
	r := mux.NewRouter()
	s := &Server{
		httpServer:           &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: r},
		router:               r,
		logger:               log.New(os.Stdout, "", log.LstdFlags),
		path:                 path,
		tables:               make(map[string]*core.Table),
		NoSync:               false,
		MaxDBs:               4096,
		MaxReaders:           126,
		StreamFlushPeriod:    60 * 1000, // milliseconds
		StreamFlushThreshold: 1000,
	}

	s.addHandlers()
	s.addTableHandlers()
	s.addPropertyHandlers()
	s.addEventHandlers()
	s.addObjectHandlers()
	s.addQueryHandlers()
	s.addDebugHandlers()

	return s
}

//------------------------------------------------------------------------------
//
// Properties
//
//------------------------------------------------------------------------------

// The root server path.
func (s *Server) Path() string {
	return s.path
}

// The path to the table metadata directory.
func (s *Server) TablesPath() string {
	return fmt.Sprintf("%v/tables", s.path)
}

// Generates the path for a table attached to the server.
func (s *Server) TablePath(name string) string {
	return fmt.Sprintf("%v/%v", s.TablesPath(), name)
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Lifecycle
//--------------------------------------

// Runs the server.
func (s *Server) ListenAndServe(shutdownChannel chan bool) error {
	s.shutdownChannel = shutdownChannel

	err := s.open()
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		s.close()
		return err
	}
	s.listener = listener

	s.shutdownFinished = make(chan bool)
	go func() {
		s.httpServer.Serve(s.listener)
		s.shutdownFinished <- true
	}()

	s.logger.Printf("Sky v%s is now listening on http://localhost%s\n", sky.Version, s.httpServer.Addr)

	return nil
}

// Stops the server.
func (s *Server) Shutdown() error {
	// Close servlets.
	s.close()

	// Close socket.
	if s.listener != nil {
		// Then stop the server.
		err := s.listener.Close()
		s.listener = nil
		if err != nil {
			return err
		}
	}
	// wait for server goroutine to finish
	<-s.shutdownFinished

	// Notify that the server is shutdown.
	if s.shutdownChannel != nil {
		s.shutdownChannel <- true
	}

	return nil
}

// Checks if the server is listening for new connections.
func (s *Server) Running() bool {
	return (s.listener != nil)
}

// Opens the data directory and servlets.
func (s *Server) open() error {
	s.close()

	// Setup the file system if it doesn't exist.
	err := s.createIfNotExists()
	if err != nil {
		return fmt.Errorf("skyd.Server: Unable to create server folders: %v", err)
	}

	// Initialize and open database.
	s.db = db.New(s.path, s.NoSync, s.MaxDBs, s.MaxReaders)
	if err = s.db.Open(); err != nil {
		s.close()
		return err
	}

	return nil
}

// Closes the database.
func (s *Server) close() {
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
}

// Creates the appropriate directory structure if one does not exist.
func (s *Server) createIfNotExists() error {
	// Create root directory.
	err := os.MkdirAll(s.path, 0700)
	if err != nil {
		return err
	}

	// Create tables directory.
	err = os.MkdirAll(s.TablesPath(), 0700)
	if err != nil {
		return err
	}

	return nil
}

//--------------------------------------
// Logging
//--------------------------------------

// Silences the log.
func (s *Server) Silence() {
	s.logger = log.New(ioutil.Discard, "", log.LstdFlags)
}

//--------------------------------------
// Routing
//--------------------------------------

// Parses incoming JSON objects and converts outgoing responses to JSON.
func (s *Server) ApiHandleFunc(route string, handlerFunction func(http.ResponseWriter, *http.Request, map[string]interface{}) (interface{}, error)) *mux.Route {
	wrappedFunction := func(w http.ResponseWriter, req *http.Request) {
		// warn("%s \"%s %s %s\"", req.RemoteAddr, req.Method, req.RequestURI, req.Proto)
		t0 := time.Now()

		var ret interface{}
		params, err := s.decodeParams(w, req)
		if err == nil {
			ret, err = handlerFunction(w, req, params)
		}

		// If we're returning plain text then just dump out what's returned.
		if _, ok := err.(*TextPlainContentTypeError); ok {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			if str, ok := ret.(string); ok {
				w.Write([]byte(str))
			}
			return
		}

		// If there is an error then replace the return value.
		if err != nil {
			ret = map[string]interface{}{"message": err.Error()}
		}

		// Write header status.
		w.Header().Set("Content-Type", "application/json")
		var status int
		if err == nil {
			status = http.StatusOK
		} else {
			status = http.StatusInternalServerError
		}
		w.WriteHeader(status)

		// Write to access log.
		s.logger.Printf("%s \"%s %s %s\" %d %0.3f", req.RemoteAddr, req.Method, req.RequestURI, req.Proto, status, time.Since(t0).Seconds())
		if status != http.StatusOK {
			s.logger.Printf("ERROR %v", err)
		}

		// Encode the return value appropriately.
		if ret != nil {
			encoder := json.NewEncoder(w)
			err := encoder.Encode(ConvertToStringKeys(ret))
			if err != nil {
				fmt.Printf("skyd.Server: Encoding error: %v\n", err.Error())
				return
			}
		}
	}

	return s.router.HandleFunc(route, wrappedFunction)
}

// Decodes the body of the message into parameters.
func (s *Server) decodeParams(w http.ResponseWriter, req *http.Request) (map[string]interface{}, error) {
	params := make(map[string]interface{})

	// If body starts with an open bracket then assume JSON. Otherwise read as raw string.
	bufReader := bufio.NewReader(req.Body)
	if first, err := bufReader.Peek(1); err == nil && string(first[0]) != "{" {
		raw, _ := ioutil.ReadAll(bufReader)
		params["RAW_POST_DATA"] = string(raw)
	} else {
		decoder := json.NewDecoder(bufReader)
		err := decoder.Decode(&params)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("Malformed json request: %v", err)
		}
	}

	return params, nil
}

//--------------------------------------
// Table Management
//--------------------------------------

// Retrieves a table that has already been opened.
func (s *Server) GetTable(name string) *core.Table {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.tables[name]
}

// Retrieves a list of all tables in the database but does not open them.
// Do not use these table references for anything but informational purposes!
func (s *Server) GetAllTables() ([]*core.Table, error) {
	// Create a table object for each directory in the tables path.
	infos, err := ioutil.ReadDir(s.TablesPath())
	if err != nil {
		return nil, err
	}

	tables := []*core.Table{}
	for _, info := range infos {
		if info.IsDir() {
			tables = append(tables, core.NewTable(info.Name(), s.TablePath(info.Name())))
		}
	}

	return tables, nil
}

// Opens a table and returns a reference to it.
func (s *Server) OpenTable(name string) (*core.Table, error) {
	// If table already exists then use it.
	table := s.GetTable(name)
	if table != nil {
		return table, nil
	}

	// Otherwise open it and save the reference.
	table = core.NewTable(name, s.TablePath(name))
	err := table.Open()
	if err != nil {
		table.Close()
		return nil, err
	}

	s.mutex.Lock()
	s.tables[name] = table
	s.mutex.Unlock()

	return table, nil
}

// Deletes a table.
func (s *Server) DeleteTable(name string) error {
	// Return an error if the table doesn't exist.
	table := s.GetTable(name)
	if table == nil {
		table = core.NewTable(name, s.TablePath(name))
	}
	if !table.Exists() {
		return fmt.Errorf("Table does not exist: %s", name)
	}

	// Delete data from the data store.
	if err := s.db.Drop(name); err != nil {
		return err
	}

	// Remove the table from the lookup and remove it's core.
	s.mutex.Lock()
	delete(s.tables, name)
	defer s.mutex.Unlock()

	return table.Delete()
}

//--------------------------------------
// Query
//--------------------------------------

// Runs a query against a table.
func (s *Server) RunQuery(table *core.Table, q *query.Query) (interface{}, error) {
	engines := make([]*query.ExecutionEngine, 0)

	// Retrieve low-level cursors for iterating.
	cursors, err := s.db.Cursors(table.Name)
	if err != nil {
		return nil, err
	}

	// Create a channel to receive aggregate responses.
	rchannel := make(chan interface{}, len(cursors))

	// Create an engine for merging results.
	rootEngine, err := query.NewExecutionEngine(q)
	if err != nil {
		return nil, err
	}
	defer rootEngine.Destroy()
	//fmt.Println(query.FullAnnotatedSource())

	// Initialize one execution engine for each servlet.
	var data interface{}
	for index, c := range cursors {
		// Create an engine for each cursor. The execution engine is
		// protected by a mutex so it's safe to destroy it at any time.
		subengine, err := query.NewExecutionEngine(q)
		if err != nil {
			return nil, err
		}
		defer subengine.Destroy()

		if err = subengine.SetLmdbCursor(c); err != nil {
			subengine.Destroy()
			cursors.Close()
			return nil, fmt.Errorf("execution engine cursor error: %s", err)
		}

		engines = append(engines, subengine)

		// Run initialization once if required.
		if index == 0 && q.RequiresInitialization() {
			if data, err = subengine.Initialize(); err != nil {
				return nil, err
			}
			// Reset the iterator.
			if err = subengine.ResetLmdbCursor(); err != nil {
				return nil, err
			}
		}
	}

	// Execute servlets asynchronously and retrieve responses outside
	// of the server context.
	for index, _ := range cursors {
		e := engines[index]
		go func() {
			if result, err := e.Aggregate(data); err != nil {
				rchannel <- err
			} else {
				rchannel <- result
			}
		}()
	}

	// Wait for each servlet to complete and then merge the results.
	var servletError error
	var result interface{}
	result = make(map[interface{}]interface{})
	for _, _ = range cursors {
		ret := <-rchannel
		if err, ok := ret.(error); ok {
			fmt.Printf("skyd.Server: Aggregate error: %v", err)
			servletError = err
		} else {
			// Defactorize aggregate results.
			if err = q.Defactorize(ret); err != nil {
				return nil, err
			}

			// Merge results.
			if ret != nil {
				if result, err = rootEngine.Merge(result, ret); err != nil {
					fmt.Printf("skyd.Server: Merge error: %v", err)
					servletError = err
				}
			}
		}
	}
	if servletError != nil {
		return nil, servletError
	}

	// Finalize results.
	if err := q.Finalize(result); err != nil {
		return nil, err
	}

	return result, nil
}
