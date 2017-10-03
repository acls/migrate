// Package postgres implements the Driver interface.
package pgx

import (
	"fmt"

	"github.com/acls/migrate/driver"
	"github.com/acls/migrate/file"
	"github.com/acls/migrate/migrate/direction"
	"github.com/jackc/pgx"
)

type Driver struct {
	// db        *sql.DB
	pool      *pgx.ConnPool
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
	return driver.initialize(pool)
}

func (driver *Driver) Initialize(url string) error {
	connConfig, err := pgx.ParseConnectionString(url)
	if err != nil {
		return err
	}
	pool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig: connConfig,
	})
	if err != nil {
		return err
	}
	return driver.initialize(pool)
}

func (driver *Driver) initialize(pool *pgx.ConnPool) error {
	driver.pool = pool

	if err := driver.ensureVersionTableExists(); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) Close() error {
	// driver.pool.Close() // don't close pool
	return nil
}

func (driver *Driver) ensureVersionTableExists() error {
	if _, err := driver.pool.Exec("CREATE TABLE IF NOT EXISTS " + driver.tableName + " (version int not null primary key);"); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "sql"
}

func (driver *Driver) Begin() (driver.Tx, error) {
	tx, err := driver.pool.Begin()
	if err != nil {
		return nil, err
	}
	return &trans{tx}, nil
}

type trans struct {
	tx *pgx.Tx
}

func (tx *trans) Exec(query string, args ...interface{}) error {
	_, err := tx.tx.Exec(query, args...)
	return err
}
func (tx *trans) Rollback() error {
	return tx.tx.Rollback()
}
func (tx *trans) Commit() error {
	return tx.tx.Commit()
}

func (driver *Driver) Migrate(tx driver.Tx, f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	if f.Direction == direction.Up {
		if err := tx.Exec("INSERT INTO "+driver.tableName+" (version) VALUES ($1)", f.Version); err != nil {
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		}
	} else if f.Direction == direction.Down {
		if err := tx.Exec("DELETE FROM "+driver.tableName+" WHERE version=$1", f.Version); err != nil {
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

	if err := tx.Exec(string(f.Content)); err != nil {
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
	err := driver.pool.QueryRow("SELECT version FROM " + driver.tableName + " ORDER BY version DESC LIMIT 1").Scan(&version)
	switch {
	case err == pgx.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return version, nil
	}
}

func init() {
	driver.RegisterDriver("postgres", &Driver{tableName: defaultTableName})
}
