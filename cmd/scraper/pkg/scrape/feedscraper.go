package scrape

import (
	"bytes"
	"context"
	"encoding/json"
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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/vllry/transit-timeseries/cmd/scraper/pkg/config"
	"github.com/vllry/transit-timeseries/pkg/types"

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

	kafkaProducer    *kafka.Producer
	kafkaStaticTopic string

	bucketName       string // Name of the GCS bucket to upload to.
	bucketPathPrefix string // Prefix for the GCS path to upload to.

	globalCompressionLock   *sync.Mutex   // Lock for compressing to limit concurrent memory usage.
	ticker                  *time.Ticker  // Ticker for the interval at which to scrape.
	quit                    chan struct{} // Channel to signal the scraper to stop gracefully.
	lockedBatchStartTime    time.Time     // Timestamp of the first entity in the current batch.
	batchStartTimeLock      sync.Mutex    // Lock for the above.
	shutdownLock            sync.RWMutex  // Protects shutdown
	compressAndUploadTicker *time.Ticker  // Ticker for the interval at which to compress.
	compressAndUploadLock   sync.Mutex
}

// NewAgencyScraper creates a new scraper for the given feed source.
func NewFeedScraper(source config.ScraperSource, globalCompressionLock *sync.Mutex, bucketName string, bucketPathPrefix string, parentOutputDirectory string, kafkaAddress string, kafkaStaticTopic string) *FeedScraper {
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

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaAddress})
	if err != nil {
		panic(err)
	}

	return &FeedScraper{
		authentication:        source.Authentication,
		bucketName:            bucketName,
		bucketPathPrefix:      bucketPathPrefix,
		batchDuration:         batchDuration,
		source:                source,
		globalCompressionLock: globalCompressionLock,
		quit:                  make(chan struct{}),
		scrapeFrequency:       scrapeFrequency,
		batchStartTimeLock:    sync.Mutex{},
		outputDirectory:       outputDirectory,
		kafkaProducer:         producer,
		kafkaStaticTopic:      kafkaStaticTopic,
	}
}

// Run periodically scrapes the source and writes the data to disk in batched directories.
// Once a batch is complete, it compresses and uploads the batch.
func (s *FeedScraper) Run() {
	defer s.kafkaProducer.Close() // TODO: is this the right place to defer?

	// Listen for Kafka events and log them.
	go func() {
		for e := range s.kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Create the output directory if it doesn't exist.
	if _, err := os.Stat(s.outputDirectory); os.IsNotExist(err) {
		err := os.Mkdir(s.outputDirectory, 0755)
		if err != nil {
			panic(err)
		}
	}

	go s.fetch() // Run the first fetch immediately.

	// Start the tickers.
	s.ticker = time.NewTicker(s.scrapeFrequency)
	s.compressAndUploadTicker = time.NewTicker(time.Minute)

	for {
		select {
		case <-s.ticker.C:
			go s.fetch()
		case <-s.compressAndUploadTicker.C:
			go s.CompressAndUploadAll()
		case <-s.quit:
			s.ticker.Stop()
			s.shutdownLock.Lock()            // Wait for writes to finish
			s.kafkaProducer.Flush(15 * 1000) // TODO: is this the right place to flush?
			fmt.Println("Shutting down scraper...")
			return
		}
	}
}

// fetch fetches the source and writes the results to disk.
func (s *FeedScraper) fetch() error {
	fetchedAt := time.Now()

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
	fmt.Println(s.source.BaseURL, s.write(body, fetchedAt))

	return nil
}

// write writes the scraped to disk in a unique sub-directory.
// The sub-directory is named after the unix timestamp of the first entity in the batch.
func (s *FeedScraper) write(b []byte, from time.Time) error {
	// Block shutdown until until the write and any uploads finish.
	s.shutdownLock.RLock()
	defer s.shutdownLock.RUnlock()

	currentDir := func() string {
		// This lock protects the batch start time from being modified in an unforseen race.
		s.batchStartTimeLock.Lock()
		defer s.batchStartTimeLock.Unlock()

		// Set a start time if there isn't one already.
		if s.lockedBatchStartTime.IsZero() {
			s.lockedBatchStartTime = from // Use the time of the first scrape.
			fmt.Println("Starting new batch", s.source.BaseURL, s.lockedBatchStartTime.Unix())
		}

		// Name the directory for THIS batch, using the current batch's start time.
		dir := fmt.Sprintf("%s/%d", s.outputDirectory, s.lockedBatchStartTime.Unix()) // TODO: dedupe. Builder function?

		// If the NEXT scrape would put us over the batch duration, mark the current batch for compression.
		// We want to do this as soon as possible, to ensure long batches are compressed in a timely manner.
		// This comparison uses a slight buffer in the scrape frequency, to compensate for some observed timing drift.
		if from.Add(time.Duration(float64(s.scrapeFrequency)*1.1)).Sub(s.lockedBatchStartTime) > s.batchDuration {
			fmt.Println("Marking end of batch", s.source.BaseURL, s.lockedBatchStartTime.Unix())

			// Set the batch start time to zero, so the next scrape will start a new batch.
			s.lockedBatchStartTime = time.Time{}
		}

		return dir
	}()

	// Create the directory if it doesn't exist.
	if _, err := os.Stat(currentDir); os.IsNotExist(err) {
		err := os.Mkdir(currentDir, 0755)
		if err != nil {
			return err
		}
	}

	err := os.WriteFile(path.Join(currentDir, fmt.Sprintf("%d", from.Unix())), b, 0644)
	if err != nil {
		return err
	}

	// Double-write static content to Kafka
	if s.source.IsStatic {
		err = s.insert(types.ScrapedStaticContent{
			Source:    s.source.BaseURL,
			Content:   b,
			ScrapedAt: from,
		})
		if err != nil {
			fmt.Println("insert err", err)
		}
	}

	// If this is a zip file (e.g. for gtfs), uncompress it in-place.
	if strings.HasSuffix(s.source.BaseURL, ".zip") {
		cmd := exec.Command("7z", "x", "-y", "-o"+currentDir, path.Join(currentDir, fmt.Sprintf("%d", from.Unix())))
		err = cmd.Run()
		if err != nil {
			return errors.Wrap(err, "couldn't uncompress")
		}

		// Remove the zip file.
		err = os.Remove(path.Join(currentDir, fmt.Sprintf("%d", from.Unix())))
		if err != nil {
			return errors.Wrap(err, "couldn't remove zip file")
		}
	}

	return nil
}

func (s *FeedScraper) CompressAndUploadAll() error {
	// This is a terrible hack to avoid concurrency issues with repeated ticker fires
	s.compressAndUploadLock.Lock()
	defer s.compressAndUploadLock.Unlock()

	// TODO: account for the fact that failures to compress will leave a start-end batch directory,
	// but compress() expects a start batch directory.

	compressErr := s.compressAll()
	if compressErr != nil {
		// If compression fails, still try to upload any archives that were already compressed.
		fmt.Println("compress failed", compressErr)
	}

	uploadErr := s.uploadAll()
	if uploadErr != nil {
		fmt.Println("upload failed", uploadErr)
	}

	if compressErr != nil {
		return compressErr
	}
	return uploadErr
}

// compressAll compresses all directories in the output directory,
// excluding the current batch directory.
func (s *FeedScraper) compressAll() error {
	files, err := os.ReadDir(s.outputDirectory)
	if err != nil {
		return errors.Wrap(err, "failed to read output directory")
	}

	// Don't compress the current batch directory.
	s.batchStartTimeLock.Lock()
	currentBatchName := fmt.Sprintf("%d", s.lockedBatchStartTime.Unix())
	s.batchStartTimeLock.Unlock()

	for _, f := range files {
		if f.IsDir() && f.Name() != currentBatchName {
			err = s.compressDir(path.Join(s.outputDirectory, f.Name()))
			if err != nil {
				fmt.Println("failed to compress", f.Name(), err)
			}
		}
	}

	return nil
}

// compressDir takes a directory named after a unix timestamp, renames it to the start-end span of the timestamped files within,
// and compresses it to a 7zip archive.
func (s *FeedScraper) compressDir(batchDir string) error {
	// compressDir expects a directory named after either a unix timestamp (batch start time),
	// or a start-end span of unix timestamps (batch start and end times).
	// TODO: this is a sign that this function is doing too much.

	dirToCompress := batchDir

	baseName := path.Base(batchDir)
	if !strings.Contains(baseName, "-") {
		// Convert the base directory name to a timestamp.
		startTimestamp, err := strconv.ParseInt(path.Base(batchDir), 10, 64)
		if err != nil {
			return errors.Wrap(err, "couldn't parse directory name")
		}

		// Find the end time of the batch from the last file in the directory.
		files, err := os.ReadDir(batchDir)
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
		dirToCompress = path.Join(path.Dir(batchDir), fmt.Sprintf("%d-%d", startTimestamp, endTimestamp))
		err = os.Rename(batchDir, dirToCompress)
		if err != nil {
			return errors.Wrap(err, "couldn't rename directory")
		}
	}

	archivePath := dirToCompress + ".7zip"

	// 7zip the directory
	err := func() error {
		s.globalCompressionLock.Lock()
		defer s.globalCompressionLock.Unlock()

		cmd := exec.Command("7z", "a", "-t7z", "-m0=lzma", "-mx=9", "-mfb=64", "-md=32m", archivePath, dirToCompress)
		err := cmd.Run()
		if err != nil {
			// Print any stdout/stderr from the command.
			var outb, errb bytes.Buffer
			cmd.Stdout = &outb
			cmd.Stderr = &errb
			fmt.Println("out:", outb.String(), "err:", errb.String())

			// Delete any partial archive.
			os.Remove(archivePath)

			return errors.Wrap(err, "couldn't compress")
		}

		fmt.Println("Compressed", dirToCompress)
		return nil
	}()

	if err != nil {
		return err
	}

	// Remove the directory
	err = os.RemoveAll(dirToCompress)
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

// Insert into Kafka
func (s *FeedScraper) insert(data types.ScrapedStaticContent) error {
	marshalled, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Produce messages to topic (asynchronously)
	err = s.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &s.kafkaStaticTopic, Partition: kafka.PartitionAny},
		Value:          marshalled,
	}, nil)

	return err
}

func (s *FeedScraper) Quit() {
	fmt.Println("Quitting scraper...")
	s.quit <- struct{}{}
}
