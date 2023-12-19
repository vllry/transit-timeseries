## Transit Timeseries Tools

This repo is for tooling to scrape realtime (and static) transit data, and to analyze that data.

## Components

### Scraper
The scraper pulls data from configured targets onto disk, compresses periodic archives, then uploads those raw archives to an object store. Storing raw data is deliberate - it allows for changes in schema design or fixing mistakes by rehydrating from the original data.

### Ingest
The ingest component will manage a database and its contents.

The scraper will dual-write to ingest and the object store, or point ingest to written objects.

## Contributing

Contributions are welcome, but this project is early enough that getting involved will mean a bunch of talking, rather than next steps being self-evident.