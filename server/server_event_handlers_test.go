package server

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

// Ensure that we can put an event on the server.
func TestServerUpdateEvents(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "factor")
		setupTestProperty("foo", "baz", true, "integer")

		// Send two new events.
		resp, _ := sendTestHttpRequest("PUT", "http://localhost:8586/tables/foo/objects/xyz/events/2012-01-01T02:00:00.123456111Z", "application/json", `{"data":{"bar":"myValue", "baz":12}}`)
		assertResponse(t, resp, 200, "", "PUT /tables/:name/objects/:objectId/events failed.")
		resp, _ = sendTestHttpRequest("PUT", "http://localhost:8586/tables/foo/objects/xyz/events/2012-01-01T03:00:00Z", "application/json", `{"data":{"bar":"myValue2"}}`)
		assertResponse(t, resp, 200, "", "PUT /tables/:name/objects/:objectId/events failed.")

		// Replace the first one.
		resp, _ = sendTestHttpRequest("PUT", "http://localhost:8586/tables/foo/objects/xyz/events/2012-01-01T02:00:00.123456222Z", "application/json", `{"data":{"bar":"myValue3", "baz":1000}}`)
		assertResponse(t, resp, 200, "", "PUT /tables/:name/objects/:objectId/events failed.")

		// Merge the second one.
		resp, _ = sendTestHttpRequest("PATCH", "http://localhost:8586/tables/foo/objects/xyz/events/2012-01-01T03:00:00Z", "application/json", `{"data":{"bar":"myValue2", "baz":20}}`)
		assertResponse(t, resp, 200, "", "PATCH /tables/:name/objects/:objectId/events failed.")

		// Check our work.
		resp, _ = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/xyz/events", "application/json", "")
		assertResponse(t, resp, 200, `[{"data":{"bar":"myValue3","baz":1000},"timestamp":"2012-01-01T02:00:00.123456Z"},{"data":{"bar":"myValue2","baz":20},"timestamp":"2012-01-01T03:00:00Z"}]`+"\n", "GET /tables/:name/objects/:objectId/events failed.")

		// Grab a single event.
		resp, _ = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/xyz/events/2012-01-01T03:00:00Z", "application/json", "")
		assertResponse(t, resp, 200, `{"data":{"bar":"myValue2","baz":20},"timestamp":"2012-01-01T03:00:00Z"}`+"\n", "GET /tables/:name/objects/:objectId/events/:timestamp failed.")
	})
}

// Ensure that we can delete all events for an object.
func TestServerDeleteEvent(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")

		// Send two new events.
		resp, _ := sendTestHttpRequest("PUT", "http://localhost:8586/tables/foo/objects/xyz/events/2012-01-01T02:00:00Z", "application/json", `{"data":{"bar":"myValue"}}`)
		assertResponse(t, resp, 200, "", "PUT /tables/:name/objects/:objectId/events failed.")
		resp, _ = sendTestHttpRequest("PUT", "http://localhost:8586/tables/foo/objects/xyz/events/2012-01-01T03:00:00Z", "application/json", `{"data":{"bar":"myValue2"}}`)
		assertResponse(t, resp, 200, "", "PUT /tables/:name/objects/:objectId/events failed.")

		// Delete one of the events.
		resp, _ = sendTestHttpRequest("DELETE", "http://localhost:8586/tables/foo/objects/xyz/events/2012-01-01T02:00:00Z", "application/json", "")
		assertResponse(t, resp, 200, "", "DELETE /tables/:name/objects/:objectId/events failed.")

		// Check our work.
		resp, _ = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/xyz/events", "application/json", "")
		assertResponse(t, resp, 200, `[{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`+"\n", "GET /tables/:name/objects/:objectId/events failed.")
	})
}

// Ensure that we can delete all events for an object.
func TestServerDeleteEvents(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")

		// Send two new events.
		resp, _ := sendTestHttpRequest("PUT", "http://localhost:8586/tables/foo/objects/xyz/events/2012-01-01T02:00:00Z", "application/json", `{"data":{"bar":"myValue"}}`)
		assertResponse(t, resp, 200, "", "PUT /tables/:name/objects/:objectId/events failed.")
		resp, _ = sendTestHttpRequest("PUT", "http://localhost:8586/tables/foo/objects/xyz/events/2012-01-01T03:00:00Z", "application/json", `{"data":{"bar":"myValue2"}}`)
		assertResponse(t, resp, 200, "", "PUT /tables/:name/objects/:objectId/events failed.")

		// Delete the events.
		resp, _ = sendTestHttpRequest("DELETE", "http://localhost:8586/tables/foo/objects/xyz/events", "application/json", "")
		assertResponse(t, resp, 200, "", "DELETE /tables/:name/objects/:objectId/events failed.")

		// Check our work.
		resp, _ = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/xyz/events", "application/json", "")
		assertResponse(t, resp, 200, "[]\n", "GET /tables/:name/objects/:objectId/events failed.")
	})
}

// Ensure that we can put multiple events on the server at once.
func TestServerStreamUpdateEvents(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")
		setupTestProperty("foo", "baz", true, "integer")

		// Send two new events in one request.
		resp, _ := sendTestHttpRequest("PATCH", "http://localhost:8586/tables/foo/events", "application/json", `{"id":"xyz","timestamp":"2012-01-01T02:00:00Z","data":{"bar":"myValue", "baz":12}}{"id":"xyz","timestamp":"2012-01-01T03:00:00Z","data":{"bar":"myValue2"}}`)
		assertResponse(t, resp, 200, `{"events_written":2}`, "PATCH /tables/:name/events failed.")

		// Check our work.
		resp, _ = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/xyz/events", "application/json", "")
		assertResponse(t, resp, 200, `[{"data":{"bar":"myValue","baz":12},"timestamp":"2012-01-01T02:00:00Z"},{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`+"\n", "GET /tables/:name/objects/:objectId/events failed.")
	})
}

// Ensure that we can put multiple events on the server at once, using table agnostic event stream.
func TestServerStreamUpdateEventsTableAgnostic(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo_1")
		setupTestProperty("foo_1", "bar", false, "string")
		setupTestProperty("foo_1", "baz", true, "integer")

		setupTestTable("foo_2")
		setupTestProperty("foo_2", "bar", false, "string")
		setupTestProperty("foo_2", "baz", true, "integer")

		// Send two new events in one request.
		resp, _ := sendTestHttpRequest("PATCH", "http://localhost:8586/events", "application/json", `{"id":"xyz","table":"foo_1","timestamp":"2012-01-01T02:00:00Z","data":{"bar":"myValue", "baz":12}}{"id":"xyz","table":"foo_2","timestamp":"2012-01-01T02:00:00Z","data":{"bar":"myValue", "baz":12}}{"id":"xyz","table":"foo_1","timestamp":"2012-01-01T03:00:00Z","data":{"bar":"myValue2"}}{"id":"xyz","table":"foo_2","timestamp":"2012-01-01T03:00:00Z","data":{"bar":"myValue2"}}`)
		assertResponse(t, resp, 200, `{"events_written":4}`, "PATCH /events failed.")

		// Check our work.
		resp, _ = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo_1/objects/xyz/events", "application/json", "")
		assertResponse(t, resp, 200, `[{"data":{"bar":"myValue","baz":12},"timestamp":"2012-01-01T02:00:00Z"},{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`+"\n", "GET /tables/:name/objects/:objectId/events failed.")
		resp, _ = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo_2/objects/xyz/events", "application/json", "")
		assertResponse(t, resp, 200, `[{"data":{"bar":"myValue","baz":12},"timestamp":"2012-01-01T02:00:00Z"},{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`+"\n", "GET /tables/:name/objects/:objectId/events failed.")
	})
}

// Ensure that streamed events are flushed when count exceeds specified threshold.
func TestServerStreamUpdateEventsFlushesOnThreshold(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")
		setupTestProperty("foo", "baz", true, "integer")
		s.StreamFlushThreshold = 2
		client, err := NewStreamingClient(t, "http://localhost:8586/events")
		assert.NoError(t, err)

		// Send a single event.
		client.Write(`{"id":"xyz","table":"foo","timestamp":"2012-01-01T02:00:00Z","data":{"bar":"myValue", "baz":12}}` + "\n")

		// Assert that the event was NOT flushed
		resp, err := sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/xyz/events", "application/json", "")
		assert.NoError(t, err)
		assertResponse(t, resp, 200, `[]`+"\n", "GET /events failed.")

		// Send a second event.
		client.Write(`{"id":"xyz","table":"foo","timestamp":"2012-01-01T03:00:00Z","data":{"bar":"myValue2"}}` + "\n")

		client.Flush()

		// Assert that the events were flushed
		resp, err = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/xyz/events", "application/json", "")
		assert.NoError(t, err)
		assertResponse(t, resp, 200, `[{"data":{"bar":"myValue","baz":12},"timestamp":"2012-01-01T02:00:00Z"},{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`+"\n", "GET /events failed.")

		// Close streaming request.
		ret := client.Close()

		// Assert that 2 events were written during stream.
		resp = ret.(*http.Response)
		assertResponse(t, resp, 200, `{"events_written":2}`, "PATCH /events failed.")

		// Ensure events exist.
		resp, err = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/xyz/events", "application/json", "")
		assert.NoError(t, err)
		assertResponse(t, resp, 200, `[{"data":{"bar":"myValue","baz":12},"timestamp":"2012-01-01T02:00:00Z"},{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`+"\n", "GET /events failed.")
	})
}

// Ensure that events are flushed when flushing timer fires.
func TestServerStreamUpdateEventsFlushesOnPeriod(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")
		setupTestProperty("foo", "baz", true, "integer")
		s.StreamFlushPeriod = 100 // milliseconds
		client, err := NewStreamingClient(t, "http://localhost:8586/events")
		assert.NoError(t, err)

		// Send a single event.
		client.Write(`{"id":"xyz","table":"foo","timestamp":"2012-01-01T02:00:00Z","data":{"bar":"myValue", "baz":12}}` + "\n")

		// Send a second event.
		client.Write(`{"id":"xyz","table":"foo","timestamp":"2012-01-01T03:00:00Z","data":{"bar":"myValue2"}}` + "\n")

		client.Flush()

		time.Sleep(2 * time.Duration(s.StreamFlushPeriod) * time.Millisecond)

		// Assert that the events were flushed
		resp, err := sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/xyz/events", "application/json", "")
		assert.NoError(t, err)
		assertResponse(t, resp, 200, `[{"data":{"bar":"myValue","baz":12},"timestamp":"2012-01-01T02:00:00Z"},{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`+"\n", "GET /events failed.")

		ret := client.Close()

		// Assert that 2 events were written during stream.
		resp = ret.(*http.Response)
		assertResponse(t, resp, 200, `{"events_written":2}`, "PATCH /events failed.")

		// Ensure events exist.
		resp, err = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/xyz/events", "application/json", "")
		assert.NoError(t, err)
		assertResponse(t, resp, 200, `[{"data":{"bar":"myValue","baz":12},"timestamp":"2012-01-01T02:00:00Z"},{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`+"\n", "GET /events failed.")
	})
}
