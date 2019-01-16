// Package pgx implements the Driver interface.
package pgx

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/acls/migrate/driver"
	"github.com/acls/migrate/file"
	"github.com/acls/migrate/migrate/direction"
	pipep "github.com/acls/migrate/pipe"
	"github.com/jackc/pgx"
)

type pgDriver struct {
	tableName string
}

const defaultTableName = "schema_migrations"

// New creates a new postgresql driver
func New(tableName string) driver.DumpDriver {
	d := &pgDriver{
		tableName: tableName,
	}
	if d.tableName == "" {
		d.tableName = defaultTableName
	}
	return d
}

func (d *pgDriver) NewConn(url, searchPath string) (driver.Conn, error) {
	return d.NewCopyConn(url, searchPath)
}
func (d *pgDriver) NewCopyConn(url, searchPath string) (driver.CopyConn, error) {
	connConfig, err := pgx.ParseConnectionString(url)
	if err != nil {
		return nil, err
	}
	c, err := pgx.Connect(connConfig)
	if err != nil {
		return nil, err
	}
	conn := Conn(c)
	_, err = d.SearchPath(conn, searchPath)
	return conn, err
}

// SearchPath sets and unsets the schema
func (d *pgDriver) SearchPath(conn driver.Conn, newSearchPath string) (revert func() error, err error) {
	// don't do nothin if the new search path is empty
	if newSearchPath == "" {
		revert = func() error { return nil }
		return
	}

	// get search_path
	var searchPath string
	if err = conn.QueryRow("SHOW search_path").Scan(&searchPath); err != nil {
		return
	}

	setSearchPath := func(verb, searchPath string) error {
		// set search path
		if err := conn.Exec("SET search_path TO " + searchPath); err != nil {
			// close the connection since the state is unknown
			conn.Close()
			return err
		}
		return nil
	}

	// set/revert search_path
	if searchPath != newSearchPath {
		if err = setSearchPath("set", newSearchPath); err != nil {
			return
		}
		revert = func() error { return setSearchPath("revert", searchPath) }
	} else {
		revert = func() error { return nil }
	}

	return
}

func (d *pgDriver) EnsureVersionTable(db driver.Beginner, schema string) (err error) {
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

	if schema != "" {
		if err := d.EnsureSchema(tx, schema); err != nil {
			return err
		}
	}

	versions := []func(driver.Databaser, string) error{
		ensureVersionTableV1,
		// ensureVersionTableV2,
	}
	if file.V2 {
		versions = append(versions, ensureVersionTableV2)
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
	return db.Exec("CREATE TABLE IF NOT EXISTS " + tbl + " (version INT NOT NULL PRIMARY KEY);")
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

	var ok bool
	if !file.V2 {
		ok = d.updateSchemaTableV1(db, f, pipe)
	} else {
		ok = d.updateSchemaTableV2(db, f, pipe)
	}
	if !ok {
		return
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

func (d *pgDriver) updateSchemaTableV1(db driver.Databaser, f *file.File, pipe chan interface{}) bool {
	if f.Direction == direction.Up {
		if err := db.Exec("INSERT INTO "+d.tableName+" (version) VALUES ($1)", f.Minor()); err != nil {
			pipe <- err
			return false
		}
	} else if f.Direction == direction.Down {
		if err := db.Exec("DELETE FROM "+d.tableName+" WHERE version=$1", f.Minor()); err != nil {
			pipe <- err
			return false
		}
	}
	return true
}

func (d *pgDriver) updateSchemaTableV2(db driver.Databaser, f *file.File, pipe chan interface{}) bool {
	if f.Direction == direction.Up {
		prevVersion := f.Version
		if !(f.Major() == 0 && f.Minor() <= 1) {
			// all versions except first version
			var err error
			prevVersion, err = d.Version(db)
			if err != nil {
				pipe <- err
				return false
			}
			if prevVersion.Inc(prevVersion.Major() != f.Major()).Compare(f.Version) != 0 {
				pipe <- fmt.Errorf("Unexpected previous version: %v for version %v", prevVersion, f.Version)
				return false
			}
		}
		// foreign key ensures correct order
		if err := db.Exec("INSERT INTO "+d.tableName+" (major,minor,prev_major,prev_minor) VALUES ($1,$2,$3,$4)",
			f.Major(), f.Minor(), prevVersion.Major(), prevVersion.Minor()); err != nil {
			pipe <- err
			return false
		}
	} else if f.Direction == direction.Down {
		if err := db.Exec("DELETE FROM "+d.tableName+" WHERE major=$1 AND minor=$2", f.Major(), f.Minor()); err != nil {
			pipe <- err
			return false
		}
	}
	return true
}

func (d *pgDriver) Version(db driver.RowQueryer) (version file.Version, err error) {
	defer func() {
		if err == pgx.ErrNoRows {
			err = nil
		}
	}()
	if !file.V2 {
		return d.versionV1(db)
	}
	return d.versionV2(db)
}

func (d *pgDriver) versionV1(db driver.RowQueryer) (file.Version, error) {
	var version uint64
	err := db.QueryRow("SELECT version FROM " + d.tableName + " ORDER BY version DESC LIMIT 1").Scan(&version)
	return file.NewVersion2(0, version), err
}

func (d *pgDriver) versionV2(db driver.RowQueryer) (file.Version, error) {
	var major, minor uint64
	err := db.QueryRow("SELECT major, minor FROM "+d.tableName+" ORDER BY major DESC, minor DESC LIMIT 1").Scan(&major, &minor)
	return file.NewVersion2(major, minor), err
}

func (d *pgDriver) Dump(conn driver.CopyConn, dw file.DumpWriter, schema string, pipe chan interface{}, handleInterrupts func() chan os.Signal) {
	defer close(pipe)

	if schema == "" {
		schema = "public"
	}

	tbls, err := d.getTables(conn, schema)
	if err != nil {
		pipe <- err
		return
	}

	for _, tbl := range tbls {
		pipe1 := pipep.New()
		go dumpTable(pipe1, conn, dw, schema, tbl)
		if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
			return
		}
	}
}
func (d *pgDriver) getTables(conn driver.Queryer, schema string) (tbls []string, err error) {
	rows, err := conn.Query(`SELECT
			table_name
		FROM information_schema.tables
		WHERE
			table_schema = $1
			AND table_name != $2`,
		schema,
		d.tableName,
	)
	defer rows.Close()

	var tbl string
	for rows.Next() {
		if err = rows.Scan(&tbl); err != nil {
			return
		}
		tbls = append(tbls, tbl)
	}
	return
}
func dumpTable(pipe chan interface{}, conn driver.CopyConn, dw file.DumpWriter, schema, tbl string) {
	defer close(pipe)

	tableName := pgx.Identifier{schema, tbl}.Sanitize()
	pipe <- tableName

	// open a writer
	w, err := dw.Writer(file.TablesDir, tbl)
	if err != nil {
		return
	}
	defer w.Close()
	// dump table
	time.Sleep(1 * time.Nanosecond)
	err = conn.CopyToWriter(w, "COPY "+tableName+" TO STDOUT")
	if err != nil {
		pipe <- err
		return
	}
}

// DeleteSchema drop the schema, if it exists
func (d *pgDriver) DeleteSchema(db driver.Execer, schema string) error {
	return db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
}

// EnsureSchema creates the schema
func (d *pgDriver) EnsureSchema(db driver.Execer, schema string) error {
	return db.Exec("CREATE SCHEMA IF NOT EXISTS " + schema)
}

// TruncateTables truncates all tables in schema except for the schema migrations table
func (d *pgDriver) TruncateTables(db driver.Conn, schema string) (err error) {
	if schema == "" {
		schema = "public"
	}

	tbls, err := d.getTables(db, schema)
	if err != nil {
		return
	}
	if len(tbls) == 0 {
		return fmt.Errorf("No tables to truncate in schema '%s'", schema)
	}

	var cmds []string
	const cmdFmt = "TRUNCATE TABLE %s CASCADE;"
	// const cmdFmt = "TRUNCATE TABLE %s;"
	for _, tbl := range tbls {
		cmds = append(cmds, fmt.Sprintf(cmdFmt, pgx.Identifier{schema, tbl}.Sanitize()))
	}
	cmd := strings.Join(cmds, "")
	// tx, err := db.Begin()
	// if err != nil {
	// 	return
	// }
	// defer func() {
	// 	if err != nil {
	// 		tx.Rollback()
	// 		return
	// 	}
	// 	err = tx.Commit()
	// }()
	// return tx.Exec(cmd)
	return db.Exec(cmd)
}

func (d *pgDriver) Restore(conn driver.CopyConn, dr file.DumpReader, schema string, pipe chan interface{}, handleInterrupts func() chan os.Signal) {
	defer close(pipe)

	tableFiles, err := dr.Files(file.TablesDir)
	if err != nil {
		pipe <- err
		return
	}

	// Disable foreign keys to prevent foreign key violations during import. https://stackoverflow.com/a/18709987
	if err := conn.Exec("SET session_replication_role = replica;"); err != nil {
		pipe <- err
		return
	}
	// Re-enable foreign keys for this connection.
	defer conn.Exec("SET session_replication_role = default;")

	// restore tables
	for _, o := range tableFiles {
		interrupts := handleInterrupts()
		if interrupts == nil {
			restoreTable(pipe, conn, schema, o)
			continue
		}
		pipe1 := pipep.New()
		go func() {
			defer close(pipe1)
			restoreTable(pipe1, conn, schema, o)
		}()
		if ok := pipep.WaitAndRedirect(pipe1, pipe, interrupts); !ok {
			return
		}
	}
}
func restoreTable(pipe chan interface{}, conn driver.CopyConn, schema string, o file.Opener) {
	tableName := pgx.Identifier{schema, o.Name}.Sanitize()
	pipe <- tableName

	r, err := o.Open()
	if err != nil {
		pipe <- err
		return
	}
	defer r.Close()
	if err = conn.CopyFromReader(r, "COPY "+tableName+" FROM STDIN"); err != nil {
		panic(tableName + ": " + err.Error())
		pipe <- err
		return
	}
}

// Conn wraps a postgresql connection and returns a driver.Conn
func Conn(c *pgx.Conn) driver.CopyConn {
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

func (c *conn) CopyToWriter(w io.Writer, sql string, args ...interface{}) error {
	return c.conn.CopyToWriter(w, sql, args...)
}
func (c *conn) CopyFromReader(r io.Reader, sql string, args ...interface{}) error {
	return c.conn.CopyFromReader(r, sql)
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
