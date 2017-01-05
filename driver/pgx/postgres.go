// Package postgres implements the Driver interface.
package pgx

import (
	"database/sql"
	"fmt"

	"github.com/acls/migrate/driver"
	"github.com/acls/migrate/file"
	"github.com/acls/migrate/migrate/direction"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
)

type Driver struct {
	db        *sql.DB
	tableName string
}

const defaultTableName = "schema_migrations"

func New(tableName string) *Driver {
	d := &Driver{
		tableName: tableName,
	}
	if d.tableName == "" {
		d.tableName = defaultTableName
	}
	return d
}

func (driver *Driver) InitializePool(pool *pgx.ConnPool) error {
	db, err := stdlib.OpenFromConnPool(pool)
	if err != nil {
		return err
	}
	return driver.initialize(db)
}

func (driver *Driver) Initialize(url string) error {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return err
	}
	return driver.initialize(db)
}

func (driver *Driver) initialize(db *sql.DB) error {
	if err := db.Ping(); err != nil {
		return err
	}
	driver.db = db

	if err := driver.ensureVersionTableExists(); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) Close() error {
	return driver.db.Close()
}

func (driver *Driver) ensureVersionTableExists() error {
	if _, err := driver.db.Exec("CREATE TABLE IF NOT EXISTS " + driver.tableName + " (version int not null primary key);"); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "sql"
}

func (driver *Driver) Begin() (driver.Tx, error) {
	return driver.db.Begin()
}

func (driver *Driver) Migrate(tx driver.Tx, f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	if f.Direction == direction.Up {
		if _, err := tx.Exec("INSERT INTO "+driver.tableName+" (version) VALUES ($1)", f.Version); err != nil {
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		}
	} else if f.Direction == direction.Down {
		if _, err := tx.Exec("DELETE FROM "+driver.tableName+" WHERE version=$1", f.Version); err != nil {
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		}
	}

	if err := f.ReadContent(); err != nil {
		pipe <- err
		return
	}

	if _, err := tx.Exec(string(f.Content)); err != nil {
		pqErr := err.(pgx.PgError)
		offset := int(pqErr.Position)
		if offset >= 0 {
			lineNo, columnNo := file.LineColumnFromOffset(f.Content, offset-1)
			errorPart := file.LinesBeforeAndAfter(f.Content, lineNo, 5, 5, true)
			pipe <- fmt.Errorf("%s %v: %s in line %v, column %v:\n\n%s", pqErr.Severity, pqErr.Code, pqErr.Message, lineNo, columnNo, string(errorPart))
		} else {
			pipe <- fmt.Errorf("%s %v: %s", pqErr.Severity, pqErr.Code, pqErr.Message)
		}
		if err := tx.Rollback(); err != nil {
			pipe <- err
		}
		return
	}

	if err := tx.Commit(); err != nil {
		pipe <- err
		return
	}
}

func (driver *Driver) Version() (uint64, error) {
	var version uint64
	err := driver.db.QueryRow("SELECT version FROM " + driver.tableName + " ORDER BY version DESC LIMIT 1").Scan(&version)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return version, nil
	}
}

// registering doesn't help since the driver lookup is done by the url scheme, which is 'postgres'
// func init() {
// 	driver.RegisterDriver("pgx", &Driver{tableName: defaultTableName})
// }
