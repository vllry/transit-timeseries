package main

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
)

func TestApp_hash(t *testing.T) {
	tests := []struct {
		name    string
		content []byte
		expect  string
	}{
		{
			name:    "hashes content",
			content: []byte("test string\n"),
			expect:  "37d2046a395cbfcb2712ff5c96a727b1966876080047c56717009dbbc235f566",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := App{}

			got := app.hash(tc.content)
			assert.Equal(t, tc.expect, got)
		})
	}
}

// TestApp_processMessage tests the processMessage method of the App type.
// It creates a new App with a FakeUploader, and initiates a clean Postgres database.
func TestApp_processMessage(t *testing.T) {
	type step struct {
		name      string
		message   *kafka.Message
		expectErr bool
	}

	// Expect represents a tuple that should be in the database after the test.
	// All (and only) files with the hash should be in the uploader.
	type expect struct {
		url      string
		hash     string
		unixTime int64
	}

	tests := []struct {
		name   string
		steps  []step
		expect []expect
	}{
		{
			name: "processes messages",
			steps: []step{
				{
					name: "inserts new entry",
					message: &kafka.Message{
						Value: []byte(`{"url": "http://example.com/gtfs.zip", "at": "2020-01-01 00:00:00Z", "content": "test string\n"}`),
					},
				},
				{
					name: "updates existing entry",
					message: &kafka.Message{
						Value: []byte(`{"url": "http://example.com/gtfs.zip", "at": "2020-01-01
00:00:00Z", "content": "test string\n"}`),
					},
				},
			},
			expect: []expect{
				{
					url:  "http://example.com",
					hash: "37d2046a395cbfcb2712ff5c96a727b1966876080047c56717009dbbc235f566",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uploader := NewFakeUploader()
			app := App{
				storage: uploader,
			}

			// Run all steps
			for _, step := range tc.steps {
				t.Run(step.name, func(t *testing.T) {
					err := app.processMessage(step.message)
					if step.expectErr {
						assert.Error(t, err)
					} else {
						assert.NoError(t, err)
					}
				})
			}
		})
	}
}
