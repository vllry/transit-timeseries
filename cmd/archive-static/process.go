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

	uploader Uploader
	db       Database
	consumer *kafka.Consumer
}

type AppConfig struct {
	kafkaTopic string
	gcsBucket  string
	gcsPrefix  string
	db         Database
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
	isNew, hash, err := app.checkIfNew(content)
	if err != nil {
		return err
	}

	if !isNew {
		fmt.Printf("Content is not new: %s\n", content.Url)
		return nil
	}

	// Write the content to GCS
	err = app.writeToGCS(content)
	if err != nil {
		return err
	}

	// Convert ScrapedAt from unix to time.Time
	scrapedAt := time.Unix(content.ScrapedAt, 0)

	// Insert the content into the database
	err = app.db.InsertStatic(content.Url, scrapedAt, hash)
	if err != nil {
		return err
	}

	return nil
}

// hash returns the sha256 hash of the content.
func (app *App) hash(content []byte) string {
	hash := sha256.Sum256(content)
	return fmt.Sprintf("%x", hash[:])
}

// checkIfNew checks if the static content is new, using the database.
// Content is "new" if it is different than the chronologically previous content.
// Note that this doesn't mean the content is unique - it could be the same as an older version.
// We just want to avoid storing sucessive duplicates.
// TODO: split out DB interaction and hashing into separate methods
func (app *App) checkIfNew(content types.ScrapedStaticContent) (bool, string, error) {
	hash := app.hash(content.Content)
	if len(hash) == 0 {
		return false, "", errors.New("hash is empty")
	}

	// Convert ScrapedAt from unix to time.Time
	scrapedAt := time.Unix(content.ScrapedAt, 0)

	prevHash, err := app.db.GetPreviousHash(content.Url, scrapedAt)
	if err != nil {
		return false, "", err
	}

	return hash != prevHash, hash, nil
}

// writeToGCS writes the content to GCS.
func (app *App) writeToGCS(content types.ScrapedStaticContent) error {
	return app.uploader.Upload(app.gcsBucket, app.gcsPrefix, content.Url, content.Content)
}
