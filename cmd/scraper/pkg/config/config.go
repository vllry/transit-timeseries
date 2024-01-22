package config

import "encoding/json"

type ScraperConfig struct {
	Version          string          `json:"version"`
	BucketName       string          `json:"bucketName"`
	BucketPathPrefix string          `json:"bucketPathPrefix"`
	Sources          []ScraperSource `json:"sources"`
	WorkingDirectory string          `json:"workingDirectory"`
	KafkaAddress     string          `json:"kafkaAddress"`
	KafkaStaticTopic string          `json:"kafkaStaticTopic"` // KafkaStaticTopic is the topic to which static data is published
}

// ScraperSource describes a scraping target
type ScraperSource struct {
	BaseURL                string                `json:"baseURL"`                // BaseURL is the base URL for the target, without any auth info
	IsStatic               bool                  `json:"isStatic"`               // IsStatic is true if the target is a static file that (allegedly) changes infrequently
	Authentication         ScraperAuthentication `json:"authentication"`         // Authentication is any auth info for the target
	ScrapeFrequencySeconds int                   `json:"scrapeFrequencySeconds"` // ScrapeFrequencySeconds is the frequency at which to scrape the target
	AggregationSeconds     int                   `json:"aggregationSeconds"`     // AggregationSeconds is the duration of time to aggregate data before uploading
}

type ScraperAuthentication struct {
	Headers    map[string]string `json:"headers"`
	Parameters map[string]string `json:"parameters"`
}

func ParseScraperConfig(b []byte) (*ScraperConfig, error) {
	var config ScraperConfig
	err := json.Unmarshal(b, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
