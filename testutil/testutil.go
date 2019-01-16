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
func MustInitPgx(t *testing.T, schema string) *pgx.Conn {
	conn, err := PgxConn(schema)
	if err != nil {
		t.Fatal(err)
	}

	// create test schema
	if _, err := conn.Exec(
		"DROP SCHEMA IF EXISTS " + schema + " CASCADE; " +
			"CREATE SCHEMA " + schema + ";"); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}

	return conn
}

// PgxConn init pgx connection. Use a unique schema per module
func PgxConn(schema string) (*pgx.Conn, error) {
	config, err := pgx.ParseConnectionString(PgxURL(schema))
	if err != nil {
		return nil, err
	}

	config.RuntimeParams = map[string]string{
		"search_path": schema,
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
