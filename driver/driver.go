// Package driver holds the driver interface.
package driver

import (
	"io"
	"os"

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

// Queryer defines a database that can perform queries.
type Queryer interface {
	Query(query string, args ...interface{}) (RowsScanner, error)
}

// Databaser interface
type Databaser interface {
	Execer
	RowQueryer
	Queryer
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

// CopyConn interface
type CopyConn interface {
	Copy
	Conn
}

// Copy interface
type Copy interface {
	CopyToWriter(w io.Writer, sql string, args ...interface{}) error
	CopyFromReader(r io.Reader, sql string, args ...interface{}) error
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
	// Copy creates a popy of the driver
	Copy(schema string) Driver

	// Creates and returns a connection
	NewConn(url string) (conn Conn, err error)

	// Ensure the version table exists
	EnsureVersionTable(db Beginner) (err error)

	// FilenameExtension returns the extension of the migration files.
	// The returned string must not begin with a dot.
	FilenameExtension() string

	// Schema returns the schema
	Schema() string
	// SetSchema sets the schema
	SetSchema(string)
	// Table returns the fully qualified table name used for storing schema migration versions.
	Table() string

	// TableName returns a fully qualified table name
	TableName(tbl string) string

	// Migrate is the heart of the driver.
	// It will receive a file which the driver should apply
	// to its backend or whatever. The migration function should use
	// the pipe channel to return any errors or other useful information.
	Migrate(db Databaser, file *file.Migration, pipe chan interface{})

	// Version returns the current migration version.
	Version(db RowQueryer) (version file.Version, err error)

	// GetMigrationFiles gets all migration files in the schema migrations table
	GetMigrationFiles(db Databaser) (files file.MigrationFiles, err error)

	// UpdateFiles updates the up and down file contents
	UpdateFiles(db Databaser, file *file.Migration, pipe chan interface{})
}

// DumpDriver interface
type DumpDriver interface {
	Driver
	NewCopyConn(url string) (conn CopyConn, err error)
	Dump(conn CopyConn, dw file.DumpWriter, pipe chan interface{}, handleInterrupts func() chan os.Signal)
	Restore(conn CopyConn, dr file.DumpReader, pipe chan interface{}, handleInterrupts func() chan os.Signal)
	DeleteSchema(db Execer) error
	TruncateTables(db Conn) error
}
