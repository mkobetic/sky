package main

import (
	"flag"
	"fmt"
	"github.com/skydb/sky/server"
	. "github.com/skydb/sky/skyd/config"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
)

//------------------------------------------------------------------------------
//
// Variables
//
//------------------------------------------------------------------------------

var config *Config
var configPath string

//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

//--------------------------------------
// Initialization
//--------------------------------------

func init() {
	config = NewConfig()
	flag.UintVar(&config.Port, "port", config.Port, "the port to listen on")
	flag.UintVar(&config.Port, "p", config.Port, "the port to listen on")
	flag.StringVar(&config.DataPath, "data-path", config.DataPath, "the data directory")
	flag.StringVar(&config.PidPath, "pid-path", config.PidPath, "the path to the pid file")
	flag.BoolVar(&config.NoSync, "nosync", config.NoSync, "use mdb.NOSYNC option, or not")
	flag.UintVar(&config.MaxDBs, "max-dbs", config.MaxDBs, "max number of named btrees in the database (mdb.MaxDBs)")
	flag.UintVar(&config.MaxReaders, "max-readers", config.MaxReaders, "max number of concurrenly executing queries (mdb.MaxReaders)")
	flag.StringVar(&configPath, "config", "", "the path to the config file")
	flag.UintVar(&config.StreamFlushPeriod, "stream-flush-period", config.StreamFlushPeriod, "time period on which to flush streamed events")
	flag.UintVar(&config.StreamFlushThreshold, "stream-flush-threshold", config.StreamFlushThreshold, "the maximum number of events (per table) in event stream before flush")
}

//--------------------------------------
// Main
//--------------------------------------

func main() {
	// Parse the command line arguments and load the config file (if specified).
	flag.Parse()
	if configPath != "" {
		file, err := os.Open(configPath)
		if err != nil {
			fmt.Printf("Unable to open config: %v\n", err)
			return
		}
		defer file.Close()
		if err = config.Decode(file); err != nil {
			fmt.Printf("Unable to parse config: %v\n", err)
			os.Exit(1)
		}
	}

	// Hardcore parallelism right here.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Initialize
	s := server.NewServer(config.Port, config.DataPath)
	s.NoSync = config.NoSync
	s.MaxDBs = config.MaxDBs
	s.MaxReaders = config.MaxReaders
	s.StreamFlushPeriod = config.StreamFlushPeriod
	s.StreamFlushThreshold = config.StreamFlushThreshold
	writePidFile()
	setupSignalHandlers(s)

	// Start the server up!
	c := make(chan bool)
	err := s.ListenAndServe(c)
	if err != nil {
		fmt.Printf("%v\n", err)
		cleanup(s)
		return
	}
	<-c
	cleanup(s)
}

//--------------------------------------
// Signals
//--------------------------------------

// Handles signals received from the OS.
func setupSignalHandlers(s *server.Server) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for _ = range c {
			fmt.Fprintln(os.Stderr, "Shutting down...")
			cleanup(s)
			fmt.Fprintln(os.Stderr, "Shutdown complete.")
			os.Exit(1)
		}
	}()
}

//--------------------------------------
// Utility
//--------------------------------------

// Shuts down the server socket and closes the database.
func cleanup(s *server.Server) {
	if s != nil {
		s.Shutdown()
	}
	deletePidFile()
}

// Writes a file to /var/run that contains the current process id.
func writePidFile() {
	pid := fmt.Sprintf("%d", os.Getpid())
	if err := ioutil.WriteFile(config.PidPath, []byte(pid), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to write pid file: %v\n", err)
	}
}

// Deletes the pid file.
func deletePidFile() {
	if _, err := os.Stat(config.PidPath); !os.IsNotExist(err) {
		if err = os.Remove(config.PidPath); err != nil {
			fmt.Fprintf(os.Stderr, "Unable to remove pid file: %v\n", err)
		}
	}
}
