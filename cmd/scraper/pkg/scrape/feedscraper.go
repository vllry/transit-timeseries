package scrape

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vllry/transit-timeseries/cmd/scraper/pkg/config"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
)

const (
	DefaultBatchDuration  = time.Minute * 10
	DefaultScrapeInterval = 20 * time.Second
)

// TODOS:
// Async background compression of non-current batch folders (uploadAll will pick them up)
// Re-evaluate locks on uploads
// For gtfs: uncompress zip and recompress as 7zip

// FeedScraper encapsulates a process scraping a single feed.
type FeedScraper struct {
	authentication  config.ScraperAuthentication
	source          config.ScraperSource // Source and identifiers for the feed.
	scrapeFrequency time.Duration        // Frequency at which to scrape the feed.
	batchDuration   time.Duration        // Maximum range of time to batch a set of results.
	outputDirectory string               // Parent directory to write the scraped data to.

	bucketName       string // Name of the GCS bucket to upload to.
	bucketPathPrefix string // Prefix for the GCS path to upload to.

	ticker               *time.Ticker  // Ticker for the interval at which to scrape.
	quit                 chan struct{} // Channel to signal the scraper to stop gracefully.
	lockedBatchStartTime time.Time     // Timestamp of the first entity in the current batch.
	batchStartTimeLock   sync.Mutex    // Lock for the above.
	shutdownLock         sync.RWMutex  // Protects shutdown
}

// NewAgencyScraper creates a new scraper for the given feed source.
func NewFeedScraper(source config.ScraperSource, bucketName string, bucketPathPrefix string, parentOutputDirectory string) *FeedScraper {
	scrapeFrequency := DefaultScrapeInterval
	if source.ScrapeFrequencySeconds != 0 {
		scrapeFrequency = time.Duration(source.ScrapeFrequencySeconds) * time.Second
	}

	batchDuration := DefaultBatchDuration
	if source.AggregationSeconds != 0 {
		batchDuration = time.Duration(source.AggregationSeconds) * time.Second
	}

	// Create a unique directory for this scraper's output.
	// The directory name is the URL-encoded source URL.
	// This is a hacky way to ensure that the scraper doesn't overwrite data from other scrapers.
	urlEscaped := url.PathEscape(strings.TrimPrefix(strings.TrimPrefix(source.BaseURL, "http://"), "https://"))
	outputDirectory := fmt.Sprintf("%s/%s", parentOutputDirectory, urlEscaped)

	return &FeedScraper{
		authentication:     source.Authentication,
		bucketName:         bucketName,
		bucketPathPrefix:   bucketPathPrefix,
		batchDuration:      batchDuration,
		source:             source,
		quit:               make(chan struct{}),
		scrapeFrequency:    scrapeFrequency,
		batchStartTimeLock: sync.Mutex{},
		outputDirectory:    outputDirectory,
	}
}

// Run periodically scrapes the source and writes the data to disk in batched directories.
// Once a batch is complete, it compresses and uploads the batch.
func (s *FeedScraper) Run() {
	if _, err := os.Stat(s.outputDirectory); os.IsNotExist(err) {
		err := os.Mkdir(s.outputDirectory, 0755)
		if err != nil {
			panic(err)
		}
	}

	// Check if there are batch directories still on-disk, e.g. if the scraper was restarted mid-batch.
	// If so, set the current batch start time to the start time of the most recent batch.
	// This ensures that the scraper will continue to write to the same batch directory if we're still in the same batch window.
	// If not, write() will handle it.
	files, err := os.ReadDir(s.outputDirectory)
	if err != nil {
		panic(err)
	}

	// Find the most recent batch directory.
	var mostRecent time.Time
	for _, f := range files {
		if f.IsDir() {
			i, err := strconv.ParseInt(f.Name(), 10, 64)
			if err != nil {
				continue
			}

			t := time.Unix(i, 0)
			if t.After(mostRecent) {
				mostRecent = t
			}
		}
	}

	if !mostRecent.IsZero() {
		s.lockedBatchStartTime = mostRecent
	}

	// Start the scraper.
	go s.fetch()                                 // Run the first fetch immediately.
	s.ticker = time.NewTicker(s.scrapeFrequency) // Don't start the ticker prematurely.

	for {
		select {
		case <-s.ticker.C:
			go s.fetch()
		case <-s.quit:
			s.ticker.Stop()
			s.shutdownLock.Lock() // Wait for writes to finish
			fmt.Println("Shutting down scraper...")
			return
		}
	}
}

// fetch fetches the source and writes the results to disk.
func (s *FeedScraper) fetch() error {
	now := time.Now()

	req, err := http.NewRequest("GET", s.source.BaseURL, nil)
	if err != nil {
		return err
	}

	for k, v := range s.source.Authentication.Headers {
		req.Header.Set(k, v)
	}

	if len(s.source.Authentication.Parameters) > 0 {
		q := req.URL.Query()
		for k, v := range s.source.Authentication.Parameters {
			q.Add(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	fmt.Println(s.source.BaseURL, s.write(body, now))

	return nil
}

// write writes the scraped to disk in a unique sub-directory.
// The sub-directory is named after the unix timestamp of the first entity in the batch.
func (s *FeedScraper) write(b []byte, from time.Time) error {
	// Block shutdown until until the write and any uploads finish.
	s.shutdownLock.RLock()
	defer s.shutdownLock.RUnlock()

	// This lock protects the batch start time from being modified in an unforseen race.
	s.batchStartTimeLock.Lock()

	// Set a start time if there isn't one already.
	if s.lockedBatchStartTime.IsZero() {
		s.lockedBatchStartTime = from // Use the time of the first scrape.
	}

	// If THIS scrape would put us over the batch duration, set a new batch start time.
	// TODO: this is a hacky way to handle the case where the scraper is restarted and resumes scraping an old batch.
	// There's probably a better way to handle all this.
	if from.Sub(s.lockedBatchStartTime) > s.batchDuration {
		s.lockedBatchStartTime = from
		fmt.Println("Starting new batch", s.source.BaseURL, s.lockedBatchStartTime.Unix())
	}

	// If the NEXT scrape would put us over the batch duration, mark the current batch for compression.
	// We want to do this as soon as possible, to ensure long batches are compressed in a timely manner.
	// This comparison is buffered by a second, to crudely account for the fact that the scrape interval is not exact.
	compressDir := ""
	if from.Add(s.scrapeFrequency).Sub(s.lockedBatchStartTime) > s.batchDuration-time.Second {
		fmt.Println("Marking end of batch", s.source.BaseURL, s.lockedBatchStartTime.Unix())
		compressDir = fmt.Sprintf("%s/%d", s.outputDirectory, s.lockedBatchStartTime.Unix()) // TODO: dedupe. Builder function?
	}

	// Name the directory for this batch.
	dir := fmt.Sprintf("%s/%d", s.outputDirectory, s.lockedBatchStartTime.Unix()) // TODO: dedupe. Builder function?

	// Release lock.
	s.batchStartTimeLock.Unlock()

	// Create the directory if it doesn't exist.
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.Mkdir(dir, 0755)
		if err != nil {
			return err
		}
	}

	err := os.WriteFile(path.Join(dir, fmt.Sprintf("%d", from.Unix())), b, 0644)
	if err != nil {
		return err
	}

	// If this is a zip file (e.g. for gtfs), uncompress it in-place.
	if strings.HasSuffix(s.source.BaseURL, ".zip") {
		cmd := exec.Command("7z", "x", "-y", "-o"+dir, path.Join(dir, fmt.Sprintf("%d", from.Unix())))
		err = cmd.Run()
		if err != nil {
			return errors.Wrap(err, "couldn't uncompress")
		}

		// Remove the zip file.
		err = os.Remove(path.Join(dir, fmt.Sprintf("%d", from.Unix())))
		if err != nil {
			return errors.Wrap(err, "couldn't remove zip file")
		}
	}

	// If this is the end of a batch, immediately compress it.
	// We don't want to wait for the next scrape interval, because that could be a long time.
	if len(compressDir) != 0 {
		err := s.compressDir(compressDir)
		if err != nil {
			return errors.Wrap(err, "failed to compress")
		}
		err = s.uploadAll()
		if err != nil {
			return errors.Wrap(err, "failed to upload")
		}
	}

	return nil
}

// compressDir takes a directory named after a unix timestamp, renames it to the start-end span of the timestamped files within,
// and compresses it to a 7zip archive.
func (s *FeedScraper) compressDir(dirpath string) error {
	// Convert the base directory name to a timestamp.
	startTimestamp, err := strconv.ParseInt(path.Base(dirpath), 10, 64)
	if err != nil {
		return errors.Wrap(err, "couldn't parse directory name")
	}

	// Find the end time of the batch from the last file in the directory.
	files, err := os.ReadDir(dirpath)
	if err != nil {
		return errors.Wrap(err, "couldn't read directory")
	}

	// Find the last file in the directory to use as the end time in the batch name.
	endTimestamp := startTimestamp
	for _, f := range files {
		if !f.IsDir() {
			i, err := strconv.ParseInt(f.Name(), 10, 64)
			if err != nil {
				continue
			}

			if i > endTimestamp {
				endTimestamp = i
			}
		}
	}

	// Rename the directory to the start-end span of the batch.
	newDir := path.Join(path.Dir(dirpath), fmt.Sprintf("%d-%d", startTimestamp, endTimestamp))
	err = os.Rename(dirpath, newDir)
	if err != nil {
		return errors.Wrap(err, "couldn't rename directory")
	}

	// 7zip the directory
	cmd := exec.Command("7z", "a", "-t7z", "-m0=lzma", "-mx=9", "-mfb=64", "-md=32m", newDir+".7zip", "./"+newDir)
	err = cmd.Run()
	if err != nil {
		fmt.Println(cmd.Stdout, cmd.Stderr)
		return errors.Wrap(err, "couldn't compress")
	}

	fmt.Println("Compressed", newDir)

	// Remove the directory
	err = os.RemoveAll(newDir)
	return err
}

// Find any archives in the output directory, and upload them to GCS.
// This function is intended to catch archives that initially failed to upload.
func (s *FeedScraper) uploadAll() error {
	files, err := os.ReadDir(s.outputDirectory)
	if err != nil {
		return errors.Wrap(err, "failed to read output directory")
	}

	for _, f := range files {
		if !f.Type().IsDir() && strings.HasSuffix(f.Name(), ".7zip") {
			err = s.uploadSingle(path.Join(s.outputDirectory, f.Name()))
			if err != nil {
				fmt.Println("failed to upload", f.Name(), err)
			}
		}
	}

	return nil
}

// uploadSingle uploads a single archive to GCS, then deletes it.
func (s *FeedScraper) uploadSingle(filePath string) error {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer client.Close()

	// Open the file.
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer f.Close()

	// The object name is the bucket path prefix + the last two path components of the file path
	// (source directory name and archive name).
	splitPath := strings.Split(filePath, "/")
	if len(splitPath) < 2 {
		return fmt.Errorf("file path breaks semantic expectations")
	}
	objectName := fmt.Sprintf("%s/%s/%s", s.bucketPathPrefix, splitPath[len(splitPath)-2], splitPath[len(splitPath)-1])

	// Upload the file to GCS
	wc := client.Bucket(s.bucketName).Object(objectName).NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return fmt.Errorf("failed to copy file to bucket: %v", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("failed to close gcs writer: %v", err)
	}

	fmt.Println("Uploaded", filePath)

	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to remove file: %v", err)
	}

	return nil
}

func (s *FeedScraper) Quit() {
	fmt.Println("Quitting scraper...")
	s.quit <- struct{}{}
}
