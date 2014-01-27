package core

import (
	"bytes"
	"encoding/binary"
	"github.com/ugorji/go/codec"
	"io"
	"time"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// An Event is a state change that occurs at a particular point in time.
type Event struct {
	Timestamp time.Time
	Data      map[int64]interface{}
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// NewEvent returns a new Event.
func NewEvent(timestamp string, data map[int64]interface{}) *Event {
	if data == nil {
		data = make(map[int64]interface{})
	}

	t, _ := time.Parse(time.RFC3339, timestamp)
	return &Event{
		Timestamp: t,
		Data:      data,
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Encoding
//--------------------------------------

// Encodes an event to MsgPack format.
func (e *Event) EncodeRaw(w io.Writer) error {
	// Encode timestamp.
	if err := binary.Write(w, binary.BigEndian, ShiftTime(e.Timestamp)); err != nil {
		return err
	}

	// Encode data.
	var handle codec.MsgpackHandle
	handle.RawToString = true
	return codec.NewEncoder(w, &handle).Encode(e.Data)
}

// Encodes an event to MsgPack format and returns the byte array.
func (e *Event) MarshalRaw() ([]byte, error) {
	buffer := new(bytes.Buffer)
	if err := e.EncodeRaw(buffer); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Decodes an event from MsgPack format.
func (e *Event) DecodeRaw(r io.Reader) error {
	var timestamp int64
	if err := binary.Read(r, binary.BigEndian, &timestamp); err != nil {
		return err
	}
	e.Timestamp = UnshiftTime(timestamp).UTC()

	var handle codec.MsgpackHandle
	handle.RawToString = true
	e.Data = make(map[int64]interface{})
	if err := codec.NewDecoder(r, &handle).Decode(&e.Data); err != nil {
		return err
	}
	for k, v := range e.Data {
		e.Data[k] = normalize(v)
	}

	return nil
}

func (e *Event) UnmarshalRaw(data []byte) error {
	return e.DecodeRaw(bytes.NewBuffer(data))
}

//--------------------------------------
// Comparator
//--------------------------------------

// Compares two events for equality.
func (e *Event) Equal(x *Event) bool {
	if !e.Timestamp.Equal(x.Timestamp.Round(time.Microsecond)) {
		return false
	}
	for k, v := range e.Data {
		if normalize(v) != normalize(x.Data[k]) {
			return false
		}
	}
	for k, v := range x.Data {
		if normalize(v) != normalize(e.Data[k]) {
			return false
		}
	}
	return true
}

// Merges the data of another event into this event.
func (e *Event) Merge(a *Event) *Event {
	if e.Data == nil && a.Data != nil {
		e.Data = make(map[int64]interface{})
	}
	for k, v := range a.Data {
		e.Data[k] = v
	}
	return e
}
