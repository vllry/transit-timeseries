package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/vllry/transit-timeseries/cmd/scraper/pkg/config"
	"github.com/vllry/transit-timeseries/cmd/scraper/pkg/scrape"
)

// Scraper encapsulates a process scraping multiple feeds.
type Scraper struct {
	config          config.ScraperConfig
	feedScrapers    []*scrape.FeedScraper
	compressionLock sync.Mutex
}

// Configure the scraper and start it.
func main() {
	configFilePath := flag.String("config", "", "path to scraper config file")
	flag.Parse()

	if configFilePath == nil || *configFilePath == "" {
		panic("no config file path provided (--config)")
	}

	// Read and parse sources config file.
	configBytes, err := os.ReadFile(*configFilePath)
	if err != nil {
		panic(err)
	}
	conf, err := ParseScraperConfig(configBytes)
	if err != nil {
		panic(err)
	}

	s := NewScraper(conf)
	s.Run()
}

// ParseScraperConfig marshalls json into a ScraperConfig.
func ParseScraperConfig(b []byte) (config.ScraperConfig, error) {
	var conf config.ScraperConfig
	err := json.Unmarshal(b, &conf)
	if err != nil {
		return config.ScraperConfig{}, err
	}

	return conf, nil
}

func NewScraper(conf config.ScraperConfig) *Scraper {
	return &Scraper{
		config:       conf,
		feedScrapers: []*scrape.FeedScraper{},
	}
}

// Run starts the scraper, and scrapes feeds until it receives an interrupt signal.
func (s *Scraper) Run() error {
	if len(s.config.WorkingDirectory) == 0 {
		panic("no working directory provided")
	}

	if _, err := os.Stat(s.config.WorkingDirectory); os.IsNotExist(err) {
		err := os.Mkdir(s.config.WorkingDirectory, 0755)
		if err != nil {
			panic(err)
		}
	}

	// Start all feed scrapers.
	for _, source := range s.config.Sources {
		scraper := scrape.NewFeedScraper(source, &s.compressionLock, s.config.BucketName, s.config.BucketPathPrefix, s.config.WorkingDirectory)
		s.feedScrapers = append(s.feedScrapers, scraper)

		// Delay for a random amount of time, up to a fraction of the scrape frequency.
		delay := time.Duration(rand.Intn(int(source.ScrapeFrequencySeconds/10))) * time.Second
		fmt.Println("Starting scraper for", source.BaseURL, "in", delay, "seconds...")
		time.AfterFunc(delay, scraper.Run)
	}

	// Create a waitgroup to wait for all channels to close.
	wg := sync.WaitGroup{}
	wg.Add(len(s.feedScrapers))

	// Listen for OS signals to gracefully shutdown.
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c // Wait
		fmt.Println("Received interrupt signal, shutting down...")
		for _, scraper := range s.feedScrapers {
			scraper.Quit()
			wg.Done() // Todo: wait for scraper to finish gracefully.
		}
	}()

	wg.Wait() // Wait for all channels to close before returning
	// TODO: this should also wait on all data to be inserted/flushed.

	return nil
}

/*
func (s *Scraper) insertFeed(feed scrape.ScrapedFeed) error {
	for _, entity := range feed.Feed.Entity {
		if entity.TripUpdate != nil {

		}
		if entity.Vehicle != nil {
		}
		if entity.Alert != nil {
			// s.insertServiceAlert(*entity.Alert)
		}
	}

	return nil
}
*/

/*
// Insert gtfsrt vehicle position.
func (s *Scraper) insertVehiclePosition(ctx context.Context, agency int, vehicle gtfsrt.VehiclePosition) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}

	// Rollback is safe to call even if the tx is already closed, so if
	// the tx commits successfully, this is a no-op
	defer tx.Rollback(context.Background())

	// Insert a new vehicle position.
	tx.Exec(
		ctx,
		"INSERT INTO vehicle_positions (agency, trip_id, route_id, start_date, start_time, direction_id, stop_id, ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
		agency,
		vehicle.Trip.TripId,
		vehicle.Trip.RouteId,
		vehicle.Trip.StartDate,
		vehicle.Trip.StartTime,
		vehicle.StopId,

		vehicle.

		vehicle.StartDate,
		vehicle.StartTime,
		vehicle.StopId,
		vehicle.StopSequence,
		vehicle.Cause,
		vehicle.Effect,
		vehicle.Url,
		vehicle.HeaderText,
		vehicle.DescriptionText,
	)

	tx.Commit(context.Background())

	return nil
}*/

/*
// Insert gtfsrt service alert.
// We want to insert each distinct alert only once, and update its last_seen timestamp if it already exists.
// Alerts don't have a clear identifier - see DB schema.
func (s *Scraper) insertServiceAlert(ctx context.Context, agency string, alert gtfsrt.Alert) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}

	// Rollback is safe to call even if the tx is already closed, so if
	// the tx commits successfully, this is a no-op
	defer tx.Rollback(context.Background())

	// Try to fetch the alert.
	// Statistically, the average alert already exists.
	rows, err := tx.Query(
		"SELECT id FROM alerts WHERE agency = $1 AND trip_id = $2 AND route_id = $3 AND start_date = $4 AND start_time = $5 AND stop_id = $6 AND stop_sequence = $7 AND cause = $8 AND effect = $9 AND url = $10 AND header_text = $11 AND description_text = $12",
		agencyId,
		alert.ActivePeriod[0].Start,
		alert.ActivePeriod[0].End,

		alert.TripId,
		alert.RouteId,
		alert.StartDate,
		alert.StartTime,
		alert.StopId,
		alert.StopSequence,
		alert.Cause,
		alert.Effect,
		alert.Url,
		alert.HeaderText,
		alert.DescriptionText,
	)
	if len(rows) > 0 {
		// Update the existing alert with a new last_seen timestamp.
		if rows[0].last_seen <= time.Now().Add(time.Minute*-5) {
			tx.Exec("UPDATE alerts SET last_seen = $1 WHERE id = $2", time.Now(), rows[0].id)
		}
	} else {
		// Insert a new alert.
		tx.Exec(
			ctx,
			"INSERT INTO alerts (agency, trip_id, route_id, start_date, start_time, stop_id, stop_sequence, cause, effect, url, header_text, description_text, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
			alert.AgencyId,
			alert.TripId,
			alert.RouteId,
			alert.StartDate,
			alert.StartTime,
			alert.StopId,
			alert.StopSequence,
			alert.Cause,
			alert.Effect,
			alert.Url,
			alert.HeaderText,
			alert.DescriptionText,
		)
	}

	tx.Commit(context.Background())

	return nil
}
*/
