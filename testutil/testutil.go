package testutil

import (
	"os"
	"testing"

	"github.com/jackc/pgx"
)

var pgxURL = os.Getenv("POSTGRES_MIGRATE_TEST_URL") + "?sslmode=disable"

// PgxURL string
func PgxURL(schema string) string {
	return pgxURL + "&search_path=" + schema
}

// MustInitPgx init pgx connection. Use a unique schema per module
func MustInitPgx(t *testing.T, schema string) (*pgx.Conn, string) {
	url := PgxURL(schema)
	config, err := pgx.ParseConnectionString(url)
	if err != nil {
		t.Fatal(err)
	}

	config.RuntimeParams = map[string]string{
		"search_path": schema,
	}

	// pool, err := pgx.NewConnPool(pgx.ConnPoolConfig{ConnConfig: config})
	conn, err := pgx.Connect(config)
	if err != nil {
		t.Fatal(err)
	}

	// create test schema
	if _, err := conn.Exec(
		"DROP SCHEMA IF EXISTS " + schema + " CASCADE; " +
			"CREATE SCHEMA " + schema + ";"); err != nil {
		t.Fatal(err)
	}

	return conn, url
}
