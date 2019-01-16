// Package driver holds the driver interface.
package driver

import (
	"github.com/acls/migrate/file"
)

// Execer interface
type Execer interface {
	Exec(query string, args ...interface{}) error
}

// RowQueryer defines a database that can query a row.
type RowQueryer interface {
	QueryRow(query string, args ...interface{}) Scanner
}

// Databaser interface
type Databaser interface {
	Execer
	Query(query string, args ...interface{}) (RowsScanner, error)
	RowQueryer
}

// Beginner defines a database that can begin a transaction.
type Beginner interface {
	Begin() (Tx, error)
}

// Conn interface
type Conn interface {
	Databaser
	Beginner
	Close() error
}

// Tx interface
type Tx interface {
	Databaser
	Rollback() error
	Commit() error
}

// RowsScanner defines a type that can scan rows returned from a database query.
type RowsScanner interface {
	// Columns() ([]string, error)
	Scan(dest ...interface{}) (err error)
	Err() error
	Next() bool
	Close()
}

// Scanner defines a type that can handle basic row scanning from a db query.
type Scanner interface {
	// Columns() ([]string, error)
	Scan(dest ...interface{}) (err error)
}

// Driver is the interface type that needs to implemented by all drivers.
type Driver interface {
	// Creates and returns a connection
	NewConn(url string) (conn Conn, err error)

	// Ensure the version table exists
	EnsureVersionTable(db Beginner) (err error)

	// FilenameExtension returns the extension of the migration files.
	// The returned string must not begin with a dot.
	FilenameExtension() string

	// TableName returns the table name used for storing schema migration versions.
	TableName() string

	// Migrate is the heart of the driver.
	// It will receive a file which the driver should apply
	// to its backend or whatever. The migration function should use
	// the pipe channel to return any errors or other useful information.
	Migrate(db Databaser, file *file.File, pipe chan interface{})

	// Version returns the current migration version.
	Version(db RowQueryer) (version file.Version, err error)
}
