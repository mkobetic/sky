package skyd

import (
  "testing"
)

// Ensure that we can put an event on the server.
func TestServerPutEvent(t *testing.T) {
  runTestServer(func(s *Server) {
    setupTestTable("foo")
    setupTestProperty("foo", "bar", "object", "string")
    setupTestProperty("foo", "baz", "action", "integer")
    
    // Send two new events.
    resp, _ := sendTestHttpRequest("PUT", "http://localhost:8585/tables/foo/objects/xyz/events/2012-01-01T02:00:00Z", "application/json", `{"data":{"bar":"myValue"}, "action":{"baz":12}}`)
    assertResponse(t, resp, 200, "", "PUT /tables/:name/objects/:objectId/events failed.")
    resp, _ = sendTestHttpRequest("PUT", "http://localhost:8585/tables/foo/objects/xyz/events/2012-01-01T03:00:00Z", "application/json", `{"data":{"bar":"myValue2"}}`)
    assertResponse(t, resp, 200, "", "PUT /tables/:name/objects/:objectId/events failed.")
    
    // Replace the first one.
    resp, _ = sendTestHttpRequest("PUT", "http://localhost:8585/tables/foo/objects/xyz/events/2012-01-01T02:00:00Z", "application/json", `{"data":{"bar":"myValue3"}, "action":{"baz":1000}}`)
    assertResponse(t, resp, 200, "", "PUT /tables/:name/objects/:objectId/events failed.")

    // Check our work.
    resp, _ = sendTestHttpRequest("GET", "http://localhost:8585/tables/foo/objects/xyz/events", "application/json", "")
    assertResponse(t, resp, 200, `[{"action":{"baz":1000},"data":{"bar":"myValue3"},"timestamp":"2012-01-01T02:00:00Z"},{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`+"\n", "GET /tables/:name/objects/:objectId/events failed.")
  })
}
