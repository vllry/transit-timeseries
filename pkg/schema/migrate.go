package schema

import (
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func RunMigrations(connString string) error {
	// Get MIGRATIONS_FILE_DIR from env
	// export MIGRATIONS_FILE_DIR=/Users/vallery/Development/transit-timeseries/pkg/schema/migrations
	dirPath := os.Getenv("MIGRATIONS_FILE_DIR")
	if len(dirPath) == 0 {
		return fmt.Errorf("MIGRATIONS_FILE_DIR not found in env")
	}
	fmt.Println("migrations working dir", dirPath)

	m, err := migrate.New("file://"+dirPath, connString)
	if err != nil {
		return err
	}
	return m.Up()
}
