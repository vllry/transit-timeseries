package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vllry/transit-timeseries/pkg/types"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
)

type App struct {
	kafkaTopic string
	gcsBucket  string
	gcsPrefix  string

	storage  ObjectStore
	consumer *kafka.Consumer
}

type AppConfig struct {
	kafkaTopic string
	gcsBucket  string
	gcsPrefix  string
}

func NewApp(config AppConfig) *App {
	return &App{
		kafkaTopic: config.kafkaTopic,
		gcsBucket:  config.gcsBucket,
		gcsPrefix:  config.gcsPrefix,
	}
}

func (app *App) Run() error {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Get messages from the consumer
	err := app.consumer.SubscribeTopics([]string{app.kafkaTopic}, nil)
	if err != nil {
		return err
	}

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := app.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				err := app.processMessage(e)
				if err != nil {
					errors.Wrap(err, "failed to process message")
				}
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	return nil
}

// processMessage converts the message to a known type, and initiates further processing
func (app *App) processMessage(msg *kafka.Message) error {
	content := &types.ScrapedStaticContent{}
	err := json.Unmarshal(msg.Value, content)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal message")
	}

	return app.processStaticContent(*content)
}

// processStaticContent processes the typed message.
func (app *App) processStaticContent(content types.ScrapedStaticContent) error {
	// Check if the content is new
	// hash := app.hash(content.Content)
	return nil
}

// hash returns the sha256 hash of the content.
func hash(content []byte) string {
	hash := sha256.Sum256(content)
	return fmt.Sprintf("%x", hash[:])
}

// writeToGCS writes the content to GCS.
func (app *App) writeToGCS(content types.ScrapedStaticContent) error {
	return app.storage.Upload(app.gcsBucket, app.gcsPrefix, content.Url, content.Content)
}

func updateIndex(versions StaticSourceArchive, sha string, filepath string, observed time.Time) StaticSourceArchive {
	var overlapExists bool

	// Check for overlaps with other versions
	for _, v := range versions.Versions {
		for _, tr := range v.TimeRanges {
			if observed.After(tr.Start) && observed.Before(tr.End) {
				overlapExists = true
				break
			}
		}
		if overlapExists {
			break
		}
	}

	for i, v := range versions.Versions {
		if v.Hash == sha {
			if !overlapExists {
				// Try to extend an existing TimeRange
				extended := false
				for j, tr := range v.TimeRanges {
					// Extend end if observed time is right after current end
					if tr.End.Add(time.Second).Equal(observed) || tr.End.Equal(observed) {
						versions.Versions[i].TimeRanges[j].End = observed
						extended = true
						break
					}
					// Extend start if observed time is right before current start
					if tr.Start.Add(-time.Second).Equal(observed) {
						versions.Versions[i].TimeRanges[j].Start = observed
						extended = true
						break
					}
				}
				if !extended {
					// Add new TimeRange if no extension occurred
					versions.Versions[i].TimeRanges = append(versions.Versions[i].TimeRanges, TimeRange{Start: observed, End: observed})
				}
			} else {
				// Add new TimeRange due to overlap with other version's time range
				versions.Versions[i].TimeRanges = append(versions.Versions[i].TimeRanges, TimeRange{Start: observed, End: observed})
			}
			return versions
		}
	}

	// If SHA does not exist, add a new Version
	newVersion := Version{
		Hash:        sha,
		StoragePath: filepath,
		TimeRanges:  []TimeRange{{Start: observed, End: observed}},
	}
	versions.Versions = append(versions.Versions, newVersion)

	return versions
}
