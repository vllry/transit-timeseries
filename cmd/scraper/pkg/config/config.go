package config

import "encoding/json"

type ScraperConfig struct {
	Version          string          `json:"version"`
	BucketName       string          `json:"bucketName"`
	BucketPathPrefix string          `json:"bucketPathPrefix"`
	Sources          []ScraperSource `json:"sources"`
	WorkingDirectory string          `json:"workingDirectory"`
}

// ScraperSource describes a scraping target
// ScraperSource is a placeholder for holding auth info (e.g. MTC's 511 API requires an API key)
type ScraperSource struct {
	BaseURL                string                `json:"baseURL"`                // BaseURL is the base URL for the target, without any auth info
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
