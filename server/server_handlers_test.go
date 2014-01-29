package server

import (
	"github.com/skydb/sky"
	"testing"
)

// Ensure that we can ping the server.
func TestServerPing(t *testing.T) {
	runTestServer(func(s *Server) {
		resp, err := sendTestHttpRequest("GET", "http://localhost:8586/ping", "application/json", "")
		if err != nil {
			t.Fatalf("Unable to ping: %v", err)
		}
		assertResponse(t, resp, 200, `{"message":"ok"}`+"\n", "GET /ping failed.")
	})
}

// Ensure that we can ping the server.
func TestServerStats(t *testing.T) {
	runTestServer(func(s *Server) {
		resp, err := sendTestHttpRequest("GET", "http://localhost:8586/stats", "application/json", "")
		if err != nil {
			t.Fatalf("Unable to get stats: %v", err)
		}
		assertResponse(t, resp, 200, `[{"entries":0,"size":2199023255552,"depth":0,"transactions":{"last":0},"readers":{"max":126,"current":0},"pages":{"last":1,"size":4096,"branch":0,"leaf":0,"overflow":0}},{"entries":0,"size":2199023255552,"depth":0,"transactions":{"last":0},"readers":{"max":126,"current":0},"pages":{"last":1,"size":4096,"branch":0,"leaf":0,"overflow":0}},{"entries":0,"size":2199023255552,"depth":0,"transactions":{"last":0},"readers":{"max":126,"current":0},"pages":{"last":1,"size":4096,"branch":0,"leaf":0,"overflow":0}},{"entries":0,"size":2199023255552,"depth":0,"transactions":{"last":0},"readers":{"max":126,"current":0},"pages":{"last":1,"size":4096,"branch":0,"leaf":0,"overflow":0}}]`+"\n", "GET /stats failed.")
	})
}

func TestServerIndex(t *testing.T) {
	runTestServer(func(S *Server) {
		resp, err := sendTestHttpRequest("GET", "http://localhost:8586/", "application/json", "")
		if err != nil {
			t.Fatalf("Unable to make request: %v", err)
		}
		assertResponse(t, resp, 200, `{"sky":"welcome","version":"`+sky.Version+`"}`+"\n", "GET / failed")
	})
}

func BenchmarkPing(b *testing.B) {
	runTestServer(func(s *Server) {
		for i := 0; i < b.N; i++ {
			resp, _ := sendTestHttpRequest("GET", "http://localhost:8586/ping", "application/json", "")
			resp.Body.Close()
		}
	})
}

func BenchmarkRawPing(b *testing.B) {
	runTestServer(func(s *Server) {
		for i := 0; i < b.N; i++ {
			resp, _ := sendTestHttpRequest("GET", "http://localhost:8586/rawping", "application/json", "")
			resp.Body.Close()
		}
	})
}
