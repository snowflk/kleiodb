package sqlstorage

import (
	"errors"
	p "github.com/snowflk/kleiodb/internal/persistence"
	"github.com/stretchr/testify/assert"
	"log"
	"strings"
	"testing"
)

var s *storage

type addingEvents struct {
	data     [][]byte
	views    []string
	expected []uint64
	hasError bool
}

type addingEventsToView struct {
	viewName string
	serials  []uint64
	hasError bool
}

type addingStreams struct {
	name     string
	data     []byte
	hasError bool
}

type addingSnapshots struct {
	source string
	key    string
	data   []byte
	err    error
}

func TestStorage_CreateStream(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_CreateStream", caseCreateStream)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_GetStream(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_GetStream", caseGetStream)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_FindStream(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_FindStream", caseFindStream)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_IncrementStreamVersion(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_IncrementStreamVersion", caseIncrementStreamVersion)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_GetStreamVersion(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_GetStreamVersion", caseGetStreamVersion)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_UpdateStreamPayload(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_UpdateStreamPayload", caseUpdateStreamPayload)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_AppendEvents(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_AppendEvents", caseAppendEvents)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_AddEventsToView(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_AddEventsToView", caseAddEventsToView)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_GetEventsFromView(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_GetEventsFromView", caseGetEventsFromView)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_GetEvents(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_GetEvents", caseGetEvents)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_SaveSnapshot(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_SaveSnapshot", caseSaveSnapshot)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_GetSnapshot(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_GetSnapshot", caseGetSnapshot)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func TestStorage_FindSnapshot(t *testing.T) {
	mustInitStorage("postgres")
	t.Run("Postgres_FindSnapshot", caseFindSnapshot)
	s.db.close()

	mustInitStorage("mysql")
	t.Run("MySQL_CreateStream", caseCreateStream)
	s.db.close()
}

func caseCreateStream(t *testing.T) {
	streams := []addingStreams{
		// Positive test
		{name: "stream-1", data: []byte(`{"a":0, "b":"1"}`)},
		{name: "stream-2", data: []byte(`{"a":0, "b":"1"}`)},
		{name: "-stream-", data: []byte(`{"a":0, "b":"1"}`)},
		{name: "_stream_", data: []byte(`{"a":0, "b":"1"}`)},
		{name: "stream--__--1", data: []byte(`{"a":0, "b":"1"}`)},

		// Invalid name shouldn't be allowed
		{name: "", data: []byte(`{"a":0, "b":"1"}`), hasError: true},
		{name: " ", data: []byte(`{"a":0, "b":"1"}`), hasError: true},
		{name: "stream!", data: []byte(`{"a":0, "b":"1"}`), hasError: true},
		{name: "!", data: []byte(`{"a":0, "b":"1"}`), hasError: true},
		{name: "stream+", data: []byte(`{"a":0, "b":"1"}`), hasError: true},
		{name: "stream.1", data: []byte(`{"a":0, "b":"1"}`), hasError: true},
		{name: "stream 11", data: []byte(`{"a":0, "b":"1"}`), hasError: true},

		// Duplicated name shouldn't be added
		{name: "stream-1", data: []byte(`{"a":0, "b":"1"}`), hasError: true},
		{name: "_stream_", data: []byte(`{"a":0, "b":"1"}`), hasError: true},

		// Data "null" shouldn't be allowed
		{name: "stream-1", data: nil, hasError: true},
	}

	for _, item := range streams {
		err := s.CreateStream(item.name, item.data)
		if item.hasError {
			if err == nil {
				t.Error(item.name, "shouldn't be added")
			}
		} else {
			if err != nil {
				t.Error(item.name, "is not valid:", err.Error())
			}
		}
	}
}

func caseGetStream(t *testing.T) {
	addStreamToStorage(t)
	tests := []struct {
		streamName string
		expected   p.RawStream
		hasError   bool
	}{
		// Positive test
		{streamName: "stream-1", expected: p.RawStream{StreamName: "stream-1", Payload: []byte(`{"a":0, "b":"1"}`), Version: 0}},
		{streamName: "stream-2", expected: p.RawStream{StreamName: "stream-2", Payload: []byte(`{"a":0, "b":"1"}`), Version: 0}},
		{streamName: "global", expected: p.RawStream{StreamName: "global", Payload: []byte(`{"a":0, "b":"1"}`), Version: 0}},

		// Invalid name shouldn't be allowed
		{streamName: "", hasError: true},
		{streamName: " ", hasError: true},
		{streamName: "stream!", hasError: true},
		{streamName: "!", hasError: true},
		{streamName: "stream+", hasError: true},
		{streamName: "stream.1", hasError: true},
		{streamName: "stream 11", hasError: true},
	}
	for _, test := range tests {
		event, err := s.GetStream(test.streamName)
		if test.hasError {
			if err == nil {
				t.Error(test.streamName, "shouldn't be found")
			}
		} else {
			if err != nil {
				t.Error(test.streamName, "should be found")
			}
			assert.Equal(t, test.expected, event)
		}
	}
}

func caseFindStream(t *testing.T) {
	addStreamToStorage(t)

	tests := []struct {
		streamName string
		expected   []string
	}{
		{streamName: "%stream%", expected: []string{"stream-1", "stream-2", "stream-3"}},
		{streamName: "%st%", expected: []string{"stream-1", "stream-2", "stream-3", "__st__"}},
		{streamName: "%glo%", expected: []string{"global"}},
		{streamName: "stream", expected: []string{}},
		{streamName: "stream-1", expected: []string{"stream-1"}},
	}

	for _, test := range tests {
		event, _ := s.FindStream(p.Pattern(test.streamName))
		assert.Equal(t, test.expected, event)
	}
}

func caseIncrementStreamVersion(t *testing.T) {
	addStreamToStorage(t)

	tests := []struct {
		streamName string
		increment  uint32
		expected   uint32
		hasError   bool
	}{
		// Positive test
		{streamName: "stream-1", increment: 2, expected: 2},
		{streamName: "stream-2", increment: 1, expected: 1},
		{streamName: "stream-1", increment: 2, expected: 4},
		{streamName: "stream-1", increment: 2, expected: 6},
		{streamName: "global", increment: 2, expected: 2},
		{streamName: "stream-2", increment: 5, expected: 6},

		// Negative test
		{streamName: "stream", increment: 2, expected: 0, hasError: true},
		{streamName: "s ", increment: 5, expected: 6, hasError: true},
		{streamName: "stream.1", increment: 5, expected: 6, hasError: true},
	}
	for _, test := range tests {
		version, err := s.IncrementStreamVersion(test.streamName, test.increment)
		if test.hasError {
			if err == nil {
				t.Errorf("%s's version shoudn't be updated\n", test.streamName)
			}
		} else {
			if err != nil {
				t.Errorf("%s's version shoud be updated\n", test.streamName)
			}
			assert.Equal(t, test.expected, version)
		}
	}
}

func caseGetStreamVersion(t *testing.T) {
	addStreamToStorage(t)

	tests := []struct {
		streamName string
		expected   uint32
		hasError   bool
	}{
		// Positive test
		{streamName: "stream-1", expected: 0},
		{streamName: "stream-2", expected: 0},
		{streamName: "global", expected: 0},

		// Negative test
		{streamName: "stream", expected: 0, hasError: true},
		{streamName: "s ", expected: 6, hasError: true},
		{streamName: "stream.1", expected: 6, hasError: true},
	}
	for _, test := range tests {
		version, err := s.GetStreamVersion(test.streamName)
		if test.hasError {
			if err == nil {
				t.Errorf("%s's version shoudn't be found\n", test.streamName)
			}
		} else {
			if err != nil {
				t.Errorf("%s's version shoud be found\n", test.streamName)
			}
			assert.Equal(t, test.expected, version)
		}
	}

	// Update stream version

	testUpdateAndGetVersion := []struct {
		streamName string
		increment  uint32
		expected   uint32
	}{
		{streamName: "stream-1", increment: 2, expected: 2},
		{streamName: "stream-2", increment: 1, expected: 1},
		{streamName: "stream-1", increment: 2, expected: 4},
		{streamName: "stream-1", increment: 2, expected: 6},
		{streamName: "global", increment: 2, expected: 2},
		{streamName: "stream-2", increment: 5, expected: 6},
	}

	for _, test := range testUpdateAndGetVersion {
		_, err := s.IncrementStreamVersion(test.streamName, test.increment)
		if err != nil {
			t.Errorf("%s's version shoud be updated\n", test.streamName)
		}

		version, err := s.GetStreamVersion(test.streamName)
		if err != nil {
			t.Errorf("%s's version shoud be found\n", test.streamName)
		}
		assert.Equal(t, test.expected, version)
	}
}

func caseUpdateStreamPayload(t *testing.T) {
	addStreamToStorage(t)

	tests := []struct {
		streamName string
		payload    []byte
		expected   p.RawStream
		hasError   bool
	}{
		// Positive test
		{streamName: "stream-1", payload: []byte(`{"a1":1, "b1":"2"}`), expected: p.RawStream{"stream-1", 0, []byte(`{"a1":1, "b1":"2"}`)}},
		{streamName: "stream-1", payload: []byte(`{"a2":2, "b2":"3"}`), expected: p.RawStream{"stream-1", 0, []byte(`{"a2":2, "b2":"3"}`)}},
		{streamName: "stream-2", payload: []byte(`{"a2":2, "b2":"3"}`), expected: p.RawStream{"stream-2", 0, []byte(`{"a2":2, "b2":"3"}`)}},
		{streamName: "global", payload: []byte(`{"a2":2, "b2":"3"}`), expected: p.RawStream{"global", 0, []byte(`{"a2":2, "b2":"3"}`)}},
		{streamName: "stream-1", payload: []byte(`{"a5":10, "b6":"11"}`), expected: p.RawStream{"stream-1", 0, []byte(`{"a5":10, "b6":"11"}`)}},

		// Invalid stream name
		{streamName: " ", payload: []byte(`{"a5":10, "b6":"11"}`), hasError: true},
		{streamName: "st.1", payload: []byte(`{"a5":10, "b6":"11"}`), hasError: true},
		{streamName: " stream 1", payload: []byte(`{"a5":10, "b6":"11"}`), hasError: true},

		// Stream name does not existed
		{streamName: "stream-4", payload: []byte(`{"a5":10, "b6":"11"}`), hasError: true},
		{streamName: "stream_1", payload: []byte(`{"a5":10, "b6":"11"}`), hasError: true},

		// Data invalid
		{streamName: "stream-1", hasError: true},
		{streamName: "stream-1", payload: nil, hasError: true},
	}

	for _, test := range tests {
		err := s.UpdateStreamPayload(test.streamName, test.payload)
		if test.hasError {
			if err == nil {
				t.Error(test.streamName, "shouldn't be updated")
			}
		} else {
			if err != nil {
				t.Error(test.streamName, "should be updated")
			}

			stream, err := s.GetStream(test.streamName)
			if err != nil {
				t.Error(test.streamName, "should be found")
			}
			assert.Equal(t, test.expected, stream)
		}
	}
}

func caseAppendEvents(t *testing.T) {
	tests := []addingEvents{
		// Positive test
		{data: bytea(`{"a":"0", "b":"1"}`, `{"c":"1", "d":"2"}`), expected: []uint64{1, 2}},
		{data: bytea(`{"a":"1", "b":"2"}`, `{"c":"2", "d":"3"}`), expected: []uint64{3, 4}},
		{data: bytea(`{"a":"2", "b":"3","c":"0"}`, `{"c":"3", "d":"6"}`), views: []string{"view-0", "view-1"}, expected: []uint64{5, 6}},
		{data: bytea(`{"a":"3", "b":"4","c":"1"}`, `{"c":"4", "d":"7","e":"0"}`, `{"f":"0", "g":"1","h":"2"}`, `{"i":"0", "k":"1","l":"2"}`), expected: []uint64{7, 8, 9, 10}},
		{data: bytea(`{"a":"4", "b":"5","c":"5"}`, `{"c":"5", "d":"8","f":"1"}`, `{"f":"0", "g":"1","h":"2"}`), views: []string{"view-2", "view-1", "view-3"}, expected: []uint64{11, 12, 13}},

		// Data invalid shouldn't be allowed
		{data: nil, hasError: true},
		{hasError: true},
	}

	for _, test := range tests {
		serialNumbers, err := s.AppendEvents(test.data, test.views)
		if test.hasError {
			if err == nil {
				t.Error(stringa(test.data), "shouldn't be added")
			}
		} else {
			if err != nil {
				t.Error(stringa(test.data), "is not valid:", err.Error())
			}
			assert.Equal(t, test.expected, serialNumbers)
		}
	}
}

func caseAddEventsToView(t *testing.T) {
	events := []addingEvents{
		// Positive test
		{data: bytea(`{"a":"0", "b":"1"}`, `{"c":"1", "d":"2"}`), expected: []uint64{1, 2}},
		{data: bytea(`{"a":"1", "b":"2"}`, `{"c":"2", "d":"3"}`), expected: []uint64{3, 4}},
		{data: bytea(`{"a":"3", "b":"4","c":"1"}`, `{"c":"4", "d":"7","e":"0"}`, `{"f":"0", "g":"1","h":"2"}`, `{"i":"0", "k":"1","l":"2"}`), expected: []uint64{5, 6, 7, 8}},
	}

	for _, test := range events {
		serialNumbers, err := s.AppendEvents(test.data, test.views)
		if err != nil {
			t.Error(stringa(test.data), "is not valid:", err.Error())
		}
		assert.Equal(t, test.expected, serialNumbers)
	}

	tests := []addingEventsToView{
		// Positive test
		{viewName: "view-1", serials: []uint64{1, 2, 3}},
		{viewName: "view-1", serials: []uint64{8}},
		{viewName: "view-1", serials: []uint64{4, 6}},
		{viewName: "view-2", serials: []uint64{1, 2, 3, 4, 5, 6, 7}},
		{viewName: "view-2", serials: []uint64{8}},
		{viewName: "view-3", serials: []uint64{8}},
		{viewName: "view-4", serials: []uint64{8}},

		// Not found serial
		{viewName: "view-2", serials: []uint64{10, 11}, hasError: true},
		{viewName: "view-1", serials: []uint64{11, 12}, hasError: true},
		{viewName: "view-3", serials: []uint64{0}, hasError: true},

		// Duplicate test
		{viewName: "view-1", serials: []uint64{4, 3}, hasError: true},
		{viewName: "view-1", serials: []uint64{4, 3}, hasError: true},

		// Invalid serials
		{viewName: "view-1", serials: []uint64{}, hasError: true},
		{viewName: "view-1", serials: nil, hasError: true},
		{viewName: "view-1", hasError: true},

		// Invalid view name
		{viewName: "", serials: []uint64{1, 2, 3}, hasError: true},
		{viewName: "    ", serials: []uint64{4, 5, 6}, hasError: true},
	}

	for _, test := range tests {
		err := s.AddEventsToView(test.viewName, test.serials)
		if test.hasError {
			if err == nil {
				t.Error(test.viewName, "shouldn't be added", err)
			}
		} else {
			if err != nil {
				t.Error(test.viewName, "should  be added", err.Error())
			}
		}
	}
}

func caseGetEventsFromView(t *testing.T) {
	events := []addingEvents{
		// Positive test
		{data: bytea(`{"a":"2", "b":"3","c":"0"}`, `{"c":"3", "d":"6"}`), views: []string{"view-0", "view-1"}, expected: []uint64{1, 2}},
		{data: bytea(`{"a":"3", "b":"4","c":"1"}`, `{"c":"4", "d":"7","e":"0"}`, `{"f":"0", "g":"1","h":"2"}`, `{"i":"0", "k":"1","l":"2"}`), views: []string{"view-0", "view-2", "view-1"}, expected: []uint64{3, 4, 5, 6}},
		{data: bytea(`{"a":"4", "b":"5","c":"5"}`, `{"c":"5", "d":"8","f":"1"}`, `{"f":"0", "g":"1","h":"2"}`), views: []string{"view-2", "view-3"}, expected: []uint64{7, 8, 9}},
	}

	for _, test := range events {
		serialNumbers, err := s.AppendEvents(test.data, test.views)
		if err != nil {
			t.Error(stringa(test.data), "is not valid:", err.Error())
		}
		assert.Equal(t, test.expected, serialNumbers)
	}

	tests := []struct {
		viewName string
		offset   uint64
		limit    uint64
		expected []p.RawEvent
		hasError bool
	}{
		// Positive test
		{viewName: "view-1", offset: 0, limit: 3, expected: []p.RawEvent{
			{Serial: 1, Payload: []byte(`{"a":"2", "b":"3","c":"0"}`)},
			{Serial: 2, Payload: []byte(`{"c":"3", "d":"6"}`)},
			{Serial: 3, Payload: []byte(`{"a":"3", "b":"4","c":"1"}`)},
		}},
		{viewName: "view-1", offset: 4, limit: 8, expected: []p.RawEvent{
			{Serial: 5, Payload: []byte(`{"f":"0", "g":"1","h":"2"}`)},
			{Serial: 6, Payload: []byte(`{"i":"0", "k":"1","l":"2"}`)},
		}},
		{viewName: "view-1", offset: 8, limit: 0, expected: []p.RawEvent{}},
		{viewName: "view-3", offset: 1, limit: 2, expected: []p.RawEvent{
			{Serial: 8, Payload: []byte(`{"c":"5", "d":"8","f":"1"}`)},
			{Serial: 9, Payload: []byte(`{"f":"0", "g":"1","h":"2"}`)},
		}},
		{viewName: "view 1", offset: 0, limit: 0, expected: []p.RawEvent{}},
		{viewName: "view 23", offset: 0, limit: 0, expected: []p.RawEvent{}},

		// Invalid view name
		{viewName: "", offset: 0, limit: 0, hasError: true},
		{viewName: "   ", offset: 0, limit: 0, hasError: true},
	}

	for _, test := range tests {
		rawEvents, err := s.GetEventsFromView(test.viewName, test.offset, test.limit)
		if test.hasError {
			if err == nil {
				t.Error(test.viewName, "shouldn't be found", err)
			}
		} else {
			if err != nil {
				t.Error(test.viewName, "should be found", err.Error())
			}

			assert.Equal(t, len(rawEvents), len(test.expected))
			for i, event := range rawEvents {
				assert.Equal(t, test.expected[i].Serial, event.Serial)
				assert.Equal(t, test.expected[i].Payload, event.Payload)

			}
		}
	}
}

func caseGetEvents(t *testing.T) {
	events := []addingEvents{
		// Positive test
		{data: bytea(`{"a":"2", "b":"3","c":"0"}`, `{"c":"3", "d":"6"}`), views: []string{"view-0", "view-1"}, expected: []uint64{1, 2}},
		{data: bytea(`{"a":"3", "b":"4","c":"1"}`, `{"c":"4", "d":"7","e":"0"}`, `{"f":"0", "g":"1","h":"2"}`, `{"i":"0", "k":"1","l":"2"}`), views: []string{"view-0", "view-2", "view-1"}, expected: []uint64{3, 4, 5, 6}},
		{data: bytea(`{"a":"4", "b":"5","c":"5"}`, `{"c":"5", "d":"8","f":"1"}`, `{"f":"0", "g":"1","h":"2"}`), views: []string{"view-2", "view-3"}, expected: []uint64{7, 8, 9}},
	}

	for _, test := range events {
		serialNumbers, err := s.AppendEvents(test.data, test.views)
		if err != nil {
			t.Error(stringa(test.data), "is not valid:", err.Error())
		}
		assert.Equal(t, test.expected, serialNumbers)
	}

	tests := []struct {
		offset   uint64
		limit    uint64
		expected []p.RawEvent
		hasError bool
	}{
		// Positive test
		{offset: 0, limit: 3, expected: []p.RawEvent{
			{Serial: 1, Payload: []byte(`{"a":"2", "b":"3","c":"0"}`)},
			{Serial: 2, Payload: []byte(`{"c":"3", "d":"6"}`)},
			{Serial: 3, Payload: []byte(`{"a":"3", "b":"4","c":"1"}`)},
		}},
		{offset: 2, limit: 1, expected: []p.RawEvent{
			{Serial: 3, Payload: []byte(`{"a":"3", "b":"4","c":"1"}`)},
		}},
		{offset: 9, limit: 0, expected: []p.RawEvent{}},
	}

	for _, test := range tests {
		rawEvents, err := s.GetEvents(test.offset, test.limit)

		if err != nil {
			t.Error("Offset ", test.offset, "Limit", test.limit, "should be found")
		}

		assert.Equal(t, len(rawEvents), len(test.expected))
		for i, event := range rawEvents {
			assert.Equal(t, test.expected[i].Serial, event.Serial)
			assert.Equal(t, test.expected[i].Payload, event.Payload)
		}
	}
}

func caseSaveSnapshot(t *testing.T) {
	snapshots := []addingSnapshots{
		// Positive test
		{source: "view-1", key: "view-1-stream-1", data: []byte(`{"a":0, "b":"1"}`)},
		{source: "view-2", key: "view-2-stream-1", data: []byte(`{"a":1, "b":"2"}`)},
		{source: "view-3", key: "view-3-stream-1", data: []byte(`{"a":2, "b":"3"}`)},
		{source: "view-1", key: "view-1-stream-2", data: []byte(`{"a":3, "b":"4"}`)},

		// Invalid source shouldn't be allowed
		{source: "    ", key: "view-1-stream-1", data: []byte(`{"a":0, "b":"1"}`), err: ErrSnapshotSourceEmpty},

		// Invalid name shouldn't be allowed
		{source: "view-1", key: "  ", data: []byte(`{"a":0, "b":"1"}`), err: ErrSnapshotNameEmpty},
	}

	for _, item := range snapshots {
		err := s.SaveSnapshot(item.source, item.key, item.data)
		if item.err != nil {
			assert.Equal(t, item.err, err)
		} else {
			if err != nil {
				t.Error("From: ", item.source, "---Name:", item.key, "should be added", err.Error())
			}
		}
	}
}

func caseGetSnapshot(t *testing.T) {
	addSnapshotToStorage(t)

	tests := []struct {
		source   string
		key      string
		expected p.RawSnapshot
		err      error
	}{
		// Positive test
		{source: "view-1", key: "view-1-user-roles", expected: p.RawSnapshot{Source: "view-1", Name: "view-1-user-roles", Payload: []byte(`{"a":0, "b":"1"}`)}},
		{source: "view-3", key: "view-3-user-roles", expected: p.RawSnapshot{Source: "view-3", Name: "view-3-user-roles", Payload: []byte(`{"a":2, "b":"3"}`)}},
		{source: "view-1", key: "view-1-user-actions", expected: p.RawSnapshot{Source: "view-1", Name: "view-1-user-actions", Payload: []byte(`{"a":3, "b":"4"}`)}},
		{source: "view-1", key: "view-1-orders", expected: p.RawSnapshot{Source: "view-1", Name: "view-1-orders", Payload: []byte(`{"a":4, "b":"5"}`)}},

		{source: "view 1", key: "view-1-user-actions", expected: p.RawSnapshot{Source: "", Name: "", Payload: []byte(nil)}},
		{source: "view-3", key: "view-3 user-roles", expected: p.RawSnapshot{Source: "", Name: "", Payload: []byte(nil)}},

		// Invalid source
		{source: "  ", key: "view-1-stream-2", err: ErrSnapshotSourceEmpty},

		// Invalid name
		{source: "view-3", key: "  ", err: ErrSnapshotNameEmpty},
	}

	for _, test := range tests {
		snapshot, err := s.GetSnapshot(test.source, test.key)
		if test.err != nil {
			assert.Equal(t, test.err, err)
		} else {
			if err != nil {
				t.Error("From: ", test.source, "---Name:", test.key, "should be found", err.Error())
			}
			assert.Equal(t, test.expected.Source, snapshot.Source)
			assert.Equal(t, test.expected.Name, snapshot.Name)
			assert.Equal(t, test.expected.Payload, snapshot.Payload)
		}
	}
}

func caseFindSnapshot(t *testing.T) {
	addSnapshotToStorage(t)

	tests := []struct {
		source   string
		pattern  p.Pattern
		expected []string
		err      error
	}{
		// Positive test
		{source: "view-1", pattern: p.Pattern("%view-1%"), expected: []string{"view-1-user-roles", "view-1-user-actions", "view-1-orders"}},
		{source: "view-1", pattern: p.Pattern("%user%"), expected: []string{"view-1-user-roles", "view-1-user-actions"}},
		{source: "view-1", pattern: p.Pattern("%orders"), expected: []string{"view-1-orders"}},
		{source: "view-1", pattern: p.Pattern("view-1-user-actions"), expected: []string{"view-1-user-actions"}},

		{source: "view 3", pattern: p.Pattern("%view-1%"), expected: []string{}},
		{source: "view-1", pattern: p.Pattern("%view 1%"), expected: []string{}},

		// Invalid source
		{source: "  ", pattern: p.Pattern("%view-1%"), err: ErrSnapshotSourceEmpty},

		// Invalid name
		{source: "view-3", pattern: p.Pattern(""), err: ErrSnapshotNameEmpty},
	}

	for _, test := range tests {
		snapshotNames, err := s.FindSnapshot(test.source, test.pattern)
		if test.err != nil {
			assert.Equal(t, test.err, err)
		} else {
			if err != nil {
				t.Error("Source: ", test.source, "---Pattern:", test.pattern.String(), "should be found", err.Error())
			}
			assert.ElementsMatch(t, test.expected, snapshotNames)
		}
	}
}

func postgresDB() Options {
	return Options{
		"postgres", "localhost", 5432, "postgres", "example", "postgres",
	}
}

func mysqlDB() Options {
	return Options{
		"mysql", "localhost", 3306, "user", "example", "mysql",
	}
}

func mustInitStorage(driver string) {
	var err error

	switch driver {
	case "postgres":
		s, err = New(postgresDB())
		break
	case "mysql":
		log.Println("MYSQL")
		s, err = New(mysqlDB())
	default:
		err = errors.New("no driver was chosen")
	}
	if err != nil {
		panic(err)
	}

	if err = s.db.dropAllIndex(); err != nil {
		panic(err)
	}
	err = s.db.exec("DROP TABLE IF EXISTS snapshots CASCADE;")
	if err != nil {
		panic(err)
	}

	err = s.db.exec("DROP TABLE IF EXISTS views CASCADE;")
	if err != nil {
		panic(err)
	}

	err = s.db.exec("DROP TABLE IF EXISTS raw_events CASCADE;")
	if err != nil {
		panic(err)
	}

	err = s.db.exec("DROP TABLE IF EXISTS streams CASCADE;")
	if err != nil {
		panic(err)
	}

	err = s.db.initStreamTbl()
	if err != nil {
		panic(err)
	}

	err = s.db.initEventTbl()
	if err != nil {
		panic(err)
	}

	err = s.db.initViewTbl()
	if err != nil {
		panic(err)
	}

	err = s.db.initSnapshotTbl()
	if err != nil {
		panic(err)
	}
}

func addStreamToStorage(t *testing.T) {
	streams := []addingStreams{
		{name: "stream-1", data: []byte(`{"a":0, "b":"1"}`)},
		{name: "stream-2", data: []byte(`{"a":0, "b":"1"}`)},
		{name: "global", data: []byte(`{"a":0, "b":"1"}`)},
		{name: "stream-3", data: []byte(`{"a":0, "b":"1"}`)},
		{name: "__st__", data: []byte(`{"a":0, "b":"1"}`)},
	}

	for _, stream := range streams {
		err := s.CreateStream(stream.name, stream.data)
		if err != nil {
			t.Error(stream.name, "is not valid:", err.Error())
		}
	}
}

func addSnapshotToStorage(t *testing.T) {
	snapshots := []addingSnapshots{
		{source: "view-1", key: "view-1-user-roles", data: []byte(`{"a":0, "b":"1"}`)},
		{source: "view-2", key: "view-2-user-actions", data: []byte(`{"a":1, "b":"2"}`)},
		{source: "view-3", key: "view-3-user-roles", data: []byte(`{"a":2, "b":"3"}`)},
		{source: "view-1", key: "view-1-user-actions", data: []byte(`{"a":3, "b":"4"}`)},
		{source: "view-1", key: "view-1-orders", data: []byte(`{"a":4, "b":"5"}`)},
	}

	for _, snapShot := range snapshots {
		err := s.SaveSnapshot(snapShot.source, snapShot.key, snapShot.data)
		if err != nil {
			t.Error("From: ", snapShot.source, "---Name:", snapShot.key, err.Error())
		}
	}
}

func bytea(strs ...string) [][]byte {
	a := make([][]byte, len(strs))
	for i, str := range strs {
		a[i] = []byte(str)
	}
	return a
}

func stringa(bytea [][]byte) string {
	a := make([]string, len(bytea))
	for i, data := range bytea {
		a[i] = string(data)
	}
	return strings.Join(a, ",")
}
