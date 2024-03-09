package main

import (
	"context"
	"database/sql"
	"time"

	"github.com/jackc/pgx/v5"
)

type Database interface {
	GetPreviousHash(url string, from time.Time) (string, error)
	InsertStatic(url string, at time.Time, hash string) error
}

type PostgresDatabase struct {
	conn *pgx.Conn
}

func NewPostgresDatabase(conn *pgx.Conn) Database {
	return &PostgresDatabase{
		conn: conn,
	}
}

func (db *PostgresDatabase) InsertStatic(url string, at time.Time, hash string) error {
	_, err := db.conn.Exec(context.Background(), "INSERT INTO static (url, at, hash) VALUES ($1, $2, $3)", url, at, hash)
	return err
}

func (db *PostgresDatabase) GetPreviousHash(url string, from time.Time) (string, error) {
	var hash string
	err := db.conn.QueryRow(context.Background(), "SELECT hash FROM static WHERE url = $1 AND at <= $2 ORDER BY at DESC LIMIT 1", url, from).Scan(&hash)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return hash, err
}

/*
// FakeDatabase is a fake implementation of the Database interface,
// with added methods for testing purposes.
type FakeDatabase struct {
	entries map[string][]fakeDbEntry
}

type fakeDbEntry struct {
	hash string
	at   time.Time
}

func NewFakeDatabase() *FakeDatabase {
	return &FakeDatabase{
		entries: make(map[string][]fakeDbEntry),
	}
}

func (db *FakeDatabase) InsertStatic(url string, at time.Time, hash string) error {
	if db.entries[url] == nil {
		db.entries[url] = make([]fakeDbEntry, 1)
	}

	db.entries[url] = append(db.entries[url], fakeDbEntry{
		hash: hash,
		at:   at,
	})

	return nil
}

func (db *FakeDatabase) GetPreviousHash(url string, from time.Time) (string, error) {
	if entries, found := db.entries[url]; found {
		for _, entry := range entries {
			if !entry.at.After(from) {
				return entry.hash, nil
			}
		}
	}

	return "", nil
}

func (db *FakeDatabase) Dump() map[string][]fakeDbEntry {
	return db.entries
}
*/
