// Package pgx implements the Driver interface.
package pgx

import (
	"fmt"

	"github.com/acls/migrate/driver"
	"github.com/acls/migrate/file"
	"github.com/acls/migrate/migrate/direction"
	"github.com/jackc/pgx"
)

type pgDriver struct {
	tableName string
}

const defaultTableName = "schema_migrations"

// New creates a new postgresql driver
func New(tableName string) driver.Driver {
	d := &pgDriver{
		tableName: tableName,
	}
	if d.tableName == "" {
		d.tableName = defaultTableName
	}
	return d
}

func (d *pgDriver) NewConn(url string) (driver.Conn, error) {
	connConfig, err := pgx.ParseConnectionString(url)
	if err != nil {
		return nil, err
	}
	c, err := pgx.Connect(connConfig)
	return Conn(c), err
}

func (d *pgDriver) EnsureVersionTable(db driver.Beginner) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	versions := []func(driver.Databaser, string) error{
		ensureVersionTableV1,
		ensureVersionTableV2,
	}
	tbl := d.tableName
	for _, ensureVersion := range versions {
		if err = ensureVersion(tx, tbl); err != nil {
			return
		}
	}
	return
}
func ensureVersionTableV1(db driver.Databaser, tbl string) error {
	return db.Exec("CREATE TABLE IF NOT EXISTS " + tbl + " (version int not null primary key);")
}
func ensureVersionTableV2(db driver.Databaser, tbl string) (err error) {
	// skip if it has the major column already
	rows, err := db.Query(`
		SELECT TRUE FROM pg_attribute
		WHERE
			attrelid = '` + tbl + `'::regclass
			AND attname = 'major'
			AND NOT attisdropped
	`)
	if err != nil {
		return err
	}
	var hasMajorColumn bool
	for rows.Next() {
		err = rows.Scan(&hasMajorColumn)
		rows.Close()
		break
	}
	if err != nil {
		return
	}
	if hasMajorColumn {
		return nil
	}

	alters := []string{
		// add new columns for major/minor versions and previous major/minor version
		`ALTER TABLE ` + tbl + `
			ADD COLUMN major INT,
			ADD COLUMN minor INT,
			ADD COLUMN prev_major INT,
			ADD COLUMN prev_minor INT
		`,
		// remove primary key
		`ALTER TABLE ` + tbl + ` DROP CONSTRAINT ` + tbl + `_pkey`,
		// ensure there are no gaps in the versions to make the next step much easier
		// steps: find max version, truncate table, add versions from 1 to max version.
		`DO $$ BEGIN DECLARE max_version INTEGER; BEGIN
			SELECT version INTO max_version FROM ` + tbl + ` ORDER BY version DESC LIMIT 1;
			TRUNCATE ` + tbl + `;
			INSERT INTO ` + tbl + ` SELECT generate_series(1,max_version);
			END; END $$;`,
		// set minor and prev_minor (makes first version reference itself)
		`UPDATE ` + tbl + ` SET major = 0, minor = version, prev_major = 0, prev_minor = GREATEST(1, version-1)`,
		// make new columns require a value
		`ALTER TABLE ` + tbl + `
			ALTER COLUMN major SET NOT NULL,
			ALTER COLUMN minor SET NOT NULL,
			ALTER COLUMN prev_major SET NOT NULL,
			ALTER COLUMN prev_minor SET NOT NULL
		`,
		// add new primary key
		`ALTER TABLE ` + tbl + ` ADD CONSTRAINT ` + tbl + `_pkey PRIMARY KEY (major,minor)`,
		// add foreign key
		`ALTER TABLE ` + tbl + ` ADD CONSTRAINT ` + tbl + `_fkey FOREIGN KEY (prev_major,prev_minor) REFERENCES ` + tbl + `(major,minor)`,
		// drop old version column
		`ALTER TABLE ` + tbl + ` DROP COLUMN version`,
	}
	for _, sql := range alters {
		if err = db.Exec(sql); err != nil {
			return err
		}
	}
	return nil
}

func (d *pgDriver) FilenameExtension() string {
	return "sql"
}

func (d *pgDriver) TableName() string {
	return d.tableName
}

func (d *pgDriver) Migrate(db driver.Databaser, f *file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	if f.Direction == direction.Up {
		var args []interface{}
		if f.Major == 0 && f.Minor <= 1 {
			// first version, reference self
			args = []interface{}{f.Major, f.Minor, f.Major, f.Minor}
		} else {
			// all versions except first version
			// foreign key forces order
			prevVersion, err := d.Version(db)
			if err != nil {
				pipe <- err
				return
			}
			if prevVersion.Inc(prevVersion.Major != f.Major).Compare(f.Version) != 0 {
				pipe <- fmt.Errorf("Unexpected previous version: %v for version %v", prevVersion, f.Version)
				return
			}
			args = []interface{}{f.Major, f.Minor, prevVersion.Major, prevVersion.Minor}
		}
		if err := db.Exec("INSERT INTO "+d.tableName+" (major,minor,prev_major,prev_minor) VALUES ($1,$2,$3,$4)", args...); err != nil {
			pipe <- err
			return
		}
	} else if f.Direction == direction.Down {
		if err := db.Exec("DELETE FROM "+d.tableName+" WHERE major=$1 AND minor=$2", f.Major, f.Minor); err != nil {
			pipe <- err
			return
		}
	}

	if err := f.ReadContent(); err != nil {
		pipe <- err
		return
	}

	if err := db.Exec(string(f.Content)); err != nil {
		pqErr, ok := err.(pgx.PgError)
		if !ok {
			pipe <- err
		}
		offset := int(pqErr.Position)
		if offset >= 0 {
			lineNo, columnNo := file.LineColumnFromOffset(f.Content, offset-1)
			errorPart := file.LinesBeforeAndAfter(f.Content, lineNo, 5, 5, true)
			pipe <- fmt.Errorf("%s %v: %s in line %v, column %v:\n\n%s", pqErr.Severity, pqErr.Code, pqErr.Message, lineNo, columnNo, string(errorPart))
		} else {
			pipe <- fmt.Errorf("%s %v: %s", pqErr.Severity, pqErr.Code, pqErr.Message)
		}
		return
	}
}

func (d *pgDriver) Version(db driver.RowQueryer) (version file.Version, err error) {
	err = db.QueryRow("SELECT major, minor FROM "+d.tableName+" ORDER BY major DESC, minor DESC LIMIT 1").Scan(&version.Major, &version.Minor)
	if err == pgx.ErrNoRows {
		err = nil
	}
	return
}

// Conn wraps a postgresql connection and returns a driver.Conn
func Conn(c *pgx.Conn) driver.Conn {
	return &conn{c}
}

type conn struct {
	conn *pgx.Conn
}

func (c *conn) Begin() (driver.Tx, error) {
	tx, err := c.conn.Begin()
	if err != nil {
		return nil, err
	}
	return &trans{tx}, nil
}
func (c *conn) Close() error {
	return c.conn.Close()
}
func (c *conn) Exec(query string, args ...interface{}) error {
	_, err := c.conn.Exec(query, args...)
	return err
}
func (c *conn) Query(query string, args ...interface{}) (driver.RowsScanner, error) {
	rows, err := c.conn.Query(query, args...)
	return Rows{rows}, err
}
func (c *conn) QueryRow(query string, args ...interface{}) driver.Scanner {
	row := c.conn.QueryRow(query, args...)
	return Row{row}
}

type trans struct {
	tx *pgx.Tx
}

func (tx *trans) Exec(query string, args ...interface{}) error {
	_, err := tx.tx.Exec(query, args...)
	return err
}
func (tx *trans) Query(query string, args ...interface{}) (driver.RowsScanner, error) {
	rows, err := tx.tx.Query(query, args...)
	return Rows{rows}, err
}
func (tx *trans) QueryRow(query string, args ...interface{}) driver.Scanner {
	row := tx.tx.QueryRow(query, args...)
	return Row{row}
}
func (tx *trans) Rollback() error {
	return tx.tx.Rollback()
}
func (tx *trans) Commit() error {
	return tx.tx.Commit()
}

// Row wraps *pgx.Row which is a convenience wrapper over *pgx.Rows
type Row struct {
	*pgx.Row
}

// Scan scans the data in the row into the provided input parameters.
func (r Row) Scan(dest ...interface{}) (err error) {
	return r.Row.Scan(dest...)
}

// Columns returns the column names for the row.
func (r Row) Columns() ([]string, error) {
	return columns((*pgx.Rows)(r.Row))
}

// Rows defines Columns so that sqlstruct.Scan can be used
type Rows struct {
	*pgx.Rows
}

// Columns returns the column names for the rows.
func (r Rows) Columns() ([]string, error) {
	return columns(r.Rows)
}

func columns(rows *pgx.Rows) ([]string, error) {
	fields := rows.FieldDescriptions()
	cols := make([]string, len(fields))
	for i := range fields {
		cols[i] = fields[i].Name
	}
	return cols, nil
}
