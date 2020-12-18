package pgx

import (
	"errors"
	"fmt"
	"strings"

	"github.com/acls/migrate/driver"
	"github.com/acls/migrate/file"
	"github.com/acls/migrate/migrate"
	"github.com/jackc/pgx"
)

// MigratableDatabase interface
type MigratableDatabase interface {
	Pool() *pgx.ConnPool
	Schema() string
	Path() string
	Migrate() (schema string, fromVersion, toVersion file.Version, err error)
	Dump(file.DumpWriter) error
	Restore(file.DumpReader) error
	Revert() error
}

// MigrateSchemas migrates all the passed in migratable databases
func MigrateSchemas(migrators ...MigratableDatabase) error {
	for _, m := range migrators {
		if m == nil {
			// skip nil migrators???
			continue
		}
		if schema, _, _, err := m.Migrate(); err != nil {
			return fmt.Errorf("Failed to migrate schema(%s): %s", schema, err)
		}
	}
	return nil
}

var _ MigratableDatabase = &SchemaMigrator{}

// SchemaMigrator struct
type SchemaMigrator struct {
	*pgx.ConnPool
	BaseMigrator migrate.Migrator
}

// InitCopy makes a copy and initializes it
func (m *SchemaMigrator) InitCopy(schemaSuffix string, d driver.DumpDriver, newPool func(string) *pgx.ConnPool, ensureSchema bool) SchemaMigrator {
	migrator := *m
	// set migration driver
	migrator.BaseMigrator.Driver = d
	// append to schema
	migrator.BaseMigrator.Schema += schemaSuffix
	// get database connection for schema
	schemas := make([]string, 0, len(migrator.BaseMigrator.ExtraSchemas)+1)
	schemas = append(schemas, migrator.Schema())
	for _, schema := range migrator.BaseMigrator.ExtraSchemas {
		schemas = append(schemas, schema+schemaSuffix)
	}
	migrator.ConnPool = newPool(strings.Join(schemas, ","))
	if ensureSchema {
		_, _ = migrator.ConnPool.Exec("CREATE SCHEMA IF NOT EXISTS " + migrator.BaseMigrator.Schema)
	}
	return migrator
}

// Pool returns connection pool
func (m *SchemaMigrator) Pool() *pgx.ConnPool {
	return m.ConnPool
}

// Schema returns the base schema
func (m *SchemaMigrator) Schema() string {
	return m.BaseMigrator.Schema
}

// Path returns the base path
func (m *SchemaMigrator) Path() string {
	return m.BaseMigrator.Path
}

// Migrate migrate between schema versions
func (m *SchemaMigrator) Migrate() (schema string, fromVersion, toVersion file.Version, err error) {
	conn, err := m.Acquire()
	if err != nil {
		return
	}
	defer m.Release(conn)

	return migrateSchema(&m.BaseMigrator, Conn(conn))
}

// Dump write the database to the DumpWriter
func (m *SchemaMigrator) Dump(dw file.DumpWriter) (err error) {
	conn, err := m.Acquire()
	if err != nil {
		return
	}
	defer m.Release(conn)

	return oneError("DumpSync failed", m.BaseMigrator.DumpSync(Conn(conn), dw))
}

// StartRestore creates a migrator and schemas used during restore
func (m *SchemaMigrator) StartRestore() (migrator migrate.Migrator, schemas []string) {
	// copy the base migrator
	migrator = m.BaseMigrator
	liveSchema := migrator.Schema
	schemas = []string{
		// liveSchema + "_delete",
		liveSchema + "_bak",
		liveSchema,
		liveSchema + "_tmp",
	}
	// change schema to tmp
	migrator.Schema = schemas[2]
	return
}

// StartRevert creates a migrator and schemas used during revert
func (m *SchemaMigrator) StartRevert() (migrator migrate.Migrator, schemas []string) {
	// copy the base migrator
	migrator = m.BaseMigrator
	// set items in schemas and prevPaths
	suffixes := [4]string{
		"_tmp", // tmp  -> [drop/delete]
		"",     // live -> tmp
		"_bak", // bak  -> live
		"_tmp", // tmp  -> bak
	}
	liveSchema := migrator.Schema
	schemas = make([]string, len(suffixes))
	for i, suffix := range suffixes {
		schema := liveSchema + suffix
		schemas[i] = schema
	}
	// change schema to bak
	migrator.Schema = schemas[2]
	return
}

// Restore restores the database from the passed in DumpReader
func (m *SchemaMigrator) Restore(dr file.DumpReader) error {
	conn, err := m.Acquire()
	if err != nil {
		return err
	}
	defer m.Release(conn)
	dconn := Conn(conn)

	migrator, schemas := m.StartRestore()

	err = oneError("RestoreSync failed", migrator.RestoreSync(dconn, dr))
	if err != nil {
		return err
	}

	_, _, _, err = migrateSchema(&migrator, dconn)
	if err != nil {
		return err
	}

	return m.FinishRestore(migrator, schemas)
}

// Revert reverts the database to the previous version
func (m *SchemaMigrator) Revert() error {
	conn, err := m.Acquire()
	if err != nil {
		return err
	}
	defer m.Release(conn)
	dconn := Conn(conn)

	migrator, schemas := m.StartRevert()
	// migrate bak schema to current schema version
	_, _, _, err = migrateSchema(&migrator, dconn)
	if err != nil {
		return errors.New("Failed to migrate previous schema up: " + err.Error())
	}

	if err := m.rotateSchemas(schemas); err != nil {
		return errors.New("Failed to rotate schemas: " + err.Error())
	}

	return nil
}

// Drop rotates the schema to _bak and creates a new schema
func (m *SchemaMigrator) Drop() error {
	conn, err := m.Acquire()
	if err != nil {
		return err
	}
	defer m.Release(conn)
	dconn := Conn(conn)

	migrator, schemas := m.StartRestore()
	// recreate tmp schema
	_, err = m.Exec("DROP SCHEMA IF EXISTS " + migrator.Schema + " CASCADE; CREATE SCHEMA " + migrator.Schema + ";")
	if err != nil {
		return err
	}
	// migrate tmp schema up
	_, _, _, err = migrateSchema(&migrator, dconn)
	if err != nil {
		return err
	}
	// rotate schemas
	if err := m.rotateSchemas(schemas); err != nil {
		return err
	}
	return nil
}

// FinishRestore finishes the restore by rotating the schemas
func (m *SchemaMigrator) FinishRestore(migrator migrate.Migrator, schemas []string) error {
	// rotates schemas: tmpSchema -> liveSchema -> bakSchema -> drop
	if err := m.rotateSchemas(schemas); err != nil {
		return fmt.Errorf("Failed to rotate schemas: %v", err)
	}
	return nil
}

func migrateSchema(migrator *migrate.Migrator, dconn driver.Conn) (schema string, fromVersion, toVersion file.Version, err error) {
	schema = migrator.Schema
	fromVersion, toVersion, errs := migrator.MigrateBetweenSync(dconn)
	err = oneError(fmt.Sprintf("Failed to migrate schema(%s)", schema), errs)
	return
}

func (m *SchemaMigrator) rotateSchemas(schemas []string) (err error) {
	t, err := m.Begin()
	if err != nil {
		return
	}
	tx := &trans{t}
	return WithTransaction(tx, func() (err error) {
		// delete the first schema
		prevSchema := schemas[0]
		if err = dropSchema(tx, prevSchema); err != nil {
			return
		}
		// rename to previous schema
		for _, schema := range schemas[1:] {
			if err = renameSchema(tx, schema, prevSchema); err != nil {
				return
			}
			prevSchema = schema
		}
		return nil
	})
}
func dropSchema(d driver.Execer, schema string) error {
	return d.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE;")
}
func renameSchema(d driver.Execer, from, to string) error {
	return d.Exec("ALTER SCHEMA " + from + " RENAME TO " + to + ";")
}

func oneError(prefix string, errs []error) error {
	if len(errs) > 0 {
		var errMsgs []string
		for _, e := range errs {
			errMsgs = append(errMsgs, e.Error())
		}
		return errors.New(prefix + ": " + strings.Join(errMsgs, "; "))
	}
	return nil
}

// WithTransaction wraps a transaction and handles rollback and commit
// and recovers if there are any panics
func WithTransaction(tx driver.Tx, fn func() error) (err error) {
	defer func() {
		// turn panic into error
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%s", p)
			}
		}
		// rollback if there was an error or a panic in txFunc
		if err != nil {
			tx.Rollback()
			return
		}
		// commit succesful transaction
		err = tx.Commit()
	}()

	// this wil set `err` that is used in defer
	// to decide whether to Rollback or Commit
	return fn()
}
