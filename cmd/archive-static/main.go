package main

import "os"

func main() {
	consumerTopic := os.Getenv("KAFKA_CONSUMER_TOPIC")
	gcsBucket := os.Getenv("GCS_BUCKET")
	gcsPrefix := os.Getenv("GCS_PREFIX")

	config := AppConfig{
		kafkaTopic: consumerTopic,
		gcsBucket:  gcsBucket,
		gcsPrefix:  gcsPrefix,
	}

	app := NewApp(config)
	app.Run()
}
