package server

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func assertResponse(t *testing.T, resp *http.Response, statusCode int, content string, message string) {
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != statusCode || content != string(body) {
		t.Fatalf("%v:\nexp:[%v] %v\ngot:[%v] %v.", message, statusCode, content, resp.StatusCode, string(body))
	}
}

func sendTestHttpRequest(method string, url string, contentType string, body string) (*http.Response, error) {
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Add("Content-Type", contentType)
	return client.Do(req)
}

func runTestServerAt(path string, f func(s *Server)) {
	server := NewServer(8586, path)
	server.Silence()
	server.ListenAndServe(nil)
	defer server.Shutdown()
	f(server)
}

func runTestServer(f func(s *Server)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)
	runTestServerAt(path, f)
}

func setupTestTable(name string) {
	resp, _ := sendTestHttpRequest("POST", "http://localhost:8586/tables", "application/json", fmt.Sprintf(`{"name":"%v"}`, name))
	resp.Body.Close()
}

func setupTestProperty(tableName string, name string, transient bool, dataType string) {
	resp, _ := sendTestHttpRequest("POST", fmt.Sprintf("http://localhost:8586/tables/%v/properties", tableName), "application/json", fmt.Sprintf(`{"name":"%v", "transient":%v, "dataType":"%v"}`, name, transient, dataType))
	resp.Body.Close()
}

func setupTestData(t *testing.T, tableName string, items [][]string) {
	for i, item := range items {
		resp, _ := sendTestHttpRequest("PUT", fmt.Sprintf("http://localhost:8586/tables/%s/objects/%s/events/%s", tableName, item[0], item[1]), "application/json", item[2])
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("setupTestData[%d]: Expected 200, got %v.", i, resp.StatusCode)
		}
	}
}

func _codegen(t *testing.T, tableName string, query string) {
	resp, _ := sendTestHttpRequest("POST", fmt.Sprintf("http://localhost:8586/tables/%s/query/codegen", tableName), "application/json", query)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func _dumpObject(t *testing.T, tableName string, objectId string) {
	resp, _ := sendTestHttpRequest("GET", fmt.Sprintf("http://localhost:8586/tables/%s/objects/%s/events", tableName, objectId), "application/json", "")
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
}

type StreamingClient struct {
	client   *http.Client
	in       *io.PipeReader
	out      *io.PipeWriter
	finished chan interface{}
	t        *testing.T
}

func NewStreamingClient(t *testing.T, endpoint string) (*StreamingClient, error) {
	method := "PATCH"

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: false}}
	in, out := io.Pipe()
	req, err := http.NewRequest(method, endpoint, in)
	assert.NoError(t, err)
	req.Header.Add("Content-Type", "application/json")

	finished := make(chan interface{})
	go func() {
		resp, err := client.Do(req)
		assert.NoError(t, err)
		finished <- resp
	}()
	return &StreamingClient{client: client, in: in, out: out, finished: finished, t: t}, nil
}

func (s *StreamingClient) Write(event string) {
	_, err := io.WriteString(io.Writer(s.out), event)
	assert.NoError(s.t, err)
}

func (s *StreamingClient) Flush() {
	// NOTE: This seems to be the only way to flush the http Client buffer...
	// so here we just fill up the buffer to force a flush.
	_, err := fmt.Fprintf(s.out, "%4096s", " ")
	assert.NoError(s.t, err)
}

func (s *StreamingClient) Close() interface{} {
	defer s.in.Close()
	s.out.Close()
	select {
	case ret := <-s.finished:
		return ret
	case <-time.After(1 * time.Second):
		s.t.Fail()
	}
	return nil
}
