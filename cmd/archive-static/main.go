package main

import (
	"context"
	"database/sql"
	"os"

	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database/postgres"
	"github.com/jackc/pgx/v5"
)

func runPostgresMigrations(postgresMigrationDir string) {
	db, err := sql.Open("postgres", "postgres://localhost:5432/database?sslmode=enable")
	if err != nil {
		panic(err)
	}
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		panic(err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://"+postgresMigrationDir,
		"postgres", driver)
	if err != nil {
		panic(err)
	}
	m.Up()
}

func main() {
	consumerTopic := os.Getenv("KAFKA_CONSUMER_TOPIC")
	gcsBucket := os.Getenv("GCS_BUCKET")
	gcsPrefix := os.Getenv("GCS_PREFIX")
	postgresMigrationDir := os.Getenv("POSTGRES_MIGRATION_DIR")

	// Run Postgres migrations before starting the app.
	runPostgresMigrations(postgresMigrationDir)

	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	db := NewPostgresDatabase(conn)

	config := AppConfig{
		db:         db,
		kafkaTopic: consumerTopic,
		gcsBucket:  gcsBucket,
		gcsPrefix:  gcsPrefix,
	}

	app := NewApp(config)
	app.Run()
}
