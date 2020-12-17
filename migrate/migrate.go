// Package migrate is imported by other Go code.
// It is the entry point to all migration functions.
package migrate

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"

	"github.com/acls/migrate/driver"
	"github.com/acls/migrate/file"
	"github.com/acls/migrate/migrate/direction"
	pipep "github.com/acls/migrate/pipe"
)

// Migrator struct
type Migrator struct {
	Driver driver.Driver
	// Path for schema migrations.
	Path string
	// // Path for storing executed migrations that are used for validation and for downgrading when the versions don't exist in MigrationsPath
	// PrevPath string
	// True if a transaction should be used for each file instead of per each major version
	TxPerFile bool
	// True if the migration should be interruptable
	Interrupts bool
	// Don't validate base upfiles
	Force bool
	// Schema to use, if set
	Schema string
}

func (m *Migrator) init(conn driver.Conn, validate bool) (prevFiles, files file.MigrationFiles, err error) {
	if err = m.Driver.EnsureVersionTable(conn, m.Schema); err != nil {
		return
	}

	prevFiles, err = m.Driver.GetMigrationFiles(conn)
	if err != nil {
		return
	}

	files, err = file.ReadMigrationFiles(m.Path, m.Driver.FilenameExtension())
	if err != nil {
		return
	}
	version, err := m.Driver.Version(conn)
	if err != nil {
		return
	}
	if prevFiles.LastVersion().Compare(version) != 0 {
		panic(fmt.Errorf("Last file version %v is less than database version %v", prevFiles.LastVersion(), version))
	}

	if validate && !m.Force {
		// check that base upfiles match
		l := len(prevFiles)
		if l > len(files) {
			l = len(files)
		}
		if err = files.ValidateBaseFiles(prevFiles[:l]); err != nil {
			return
		}
	}
	return
}

// Up applies all available migrations
func (m *Migrator) Up(pipe chan interface{}, conn driver.Conn) {
	prevFiles, files, err := m.init(conn, true)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}
	m.up(pipe, conn, prevFiles, files, prevFiles.LastVersion())
}
func (m *Migrator) up(pipe chan interface{}, conn driver.Conn, prevFiles, files file.MigrationFiles, version file.Version) {
	applyMigrations := files.ToLastFrom(version)
	m.MigrateFiles(pipe, conn, prevFiles, files, applyMigrations)
}

// UpSync is synchronous version of Up
func (m *Migrator) UpSync(conn driver.Conn) []error {
	pipe := pipep.New()
	go m.Up(pipe, conn)
	return pipep.ReadErrors(pipe)
}

// Down rolls back all migrations
func (m *Migrator) Down(pipe chan interface{}, conn driver.Conn) {
	prevFiles, files, err := m.init(conn, true)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrations := files.ToFirstFrom(prevFiles.LastVersion())
	m.MigrateFiles(pipe, conn, prevFiles, files, applyMigrations)
}

// DownSync is synchronous version of Down
func (m *Migrator) DownSync(conn driver.Conn) []error {
	pipe := pipep.New()
	go m.Down(pipe, conn)
	return pipep.ReadErrors(pipe)
}

// Redo rolls back the most recently applied migration, then runs it again.
func (m *Migrator) Redo(pipe chan interface{}, conn driver.Conn) {
	pipe1 := pipep.New()
	go m.Migrate(pipe1, conn, -1)
	if ok := pipep.WaitAndRedirect(pipe1, pipe, m.handleInterrupts()); !ok {
		go pipep.Close(pipe, nil)
		return
	} else {
		go m.Migrate(pipe, conn, +1)
	}
}

// RedoSync is synchronous version of Redo
func (m *Migrator) RedoSync(conn driver.Conn) []error {
	pipe := pipep.New()
	go m.Redo(pipe, conn)
	return pipep.ReadErrors(pipe)
}

// Reset runs the down and up migration function
func (m *Migrator) Reset(pipe chan interface{}, conn driver.Conn) {
	pipe1 := pipep.New()
	go m.Down(pipe1, conn)
	if ok := pipep.WaitAndRedirect(pipe1, pipe, m.handleInterrupts()); !ok {
		go pipep.Close(pipe, nil)
		return
	} else {
		go m.Up(pipe, conn)
	}
}

// ResetSync is synchronous version of Reset
func (m *Migrator) ResetSync(conn driver.Conn) []error {
	pipe := pipep.New()
	go m.Reset(pipe, conn)
	return pipep.ReadErrors(pipe)
}

// MigrateBetween migrates to the destination version
func (m *Migrator) MigrateBetween(pipe chan interface{}, conn driver.Conn) (curVersion, dstVersion file.Version) {
	prevFiles, files, err := m.init(conn, !m.Force)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	var applyMigrations file.Migrations
	if len(prevFiles) == 0 {
		// no previous files so just migrate up or down depending on versions
		sort.Sort(files) // make sure LastVersion is correct
		curVersion = prevFiles.LastVersion()
		dstVersion = files.LastVersion()
		if curVersion.Compare(dstVersion) <= 0 { // migrate up
			applyMigrations = files.ToLastFrom(curVersion)
		} else { // migrate down
			applyMigrations = files.DownTo(dstVersion)
		}
	} else {
		// migrate between previous files and current files
		curVersion, dstVersion, applyMigrations, err = files.Between(prevFiles, m.Force)
		if err != nil {
			go pipep.Close(pipe, err)
			return
		}
		// // TODO: delete? this should be possible with file contents stored in and fetched from db
		// sort.Sort(prevFiles) // ensure correct sort
		// version := prevFiles.LastVersion()
		// if curVersion.Compare(version) != 0 {
		// 	go pipep.Close(pipe, fmt.Errorf("Database version(%v) doesn't match current migration files version(%v)", curVersion, version))
		// 	return
		// }
	}

	m.MigrateFiles(pipe, conn, prevFiles, files, applyMigrations)
	return
}

// MigrateBetweenSync is synchronous version of MigrateBetween
func (m *Migrator) MigrateBetweenSync(conn driver.Conn) (curVersion, dstVersion file.Version, errs []error) {
	pipe := pipep.New()
	go func() {
		curVersion, dstVersion = m.MigrateBetween(pipe, conn)
	}()
	errs = pipep.ReadErrors(pipe)
	return
}

// MigrateTo migrates to the destination version
func (m *Migrator) MigrateTo(pipe chan interface{}, conn driver.Conn, dstVersion file.Version) (version file.Version) {
	prevFiles, files, err := m.init(conn, true)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	version = prevFiles.LastVersion()
	applyMigrations, err := files.FromTo(version, dstVersion)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	m.MigrateFiles(pipe, conn, prevFiles, files, applyMigrations)
	return
}

// MigrateToSync is synchronous version of MigrateTo
func (m *Migrator) MigrateToSync(conn driver.Conn, dstVersion file.Version) (version file.Version, errs []error) {
	pipe := pipep.New()
	go func() {
		version = m.MigrateTo(pipe, conn, dstVersion)
	}()
	errs = pipep.ReadErrors(pipe)
	return
}

// Migrate applies relative +n/-n migrations
func (m *Migrator) Migrate(pipe chan interface{}, conn driver.Conn, relativeN int) {
	prevFiles, files, err := m.init(conn, true)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrations := files.From(prevFiles.LastVersion(), relativeN)

	if relativeN == 0 {
		applyMigrations = nil
	}

	m.MigrateFiles(pipe, conn, prevFiles, files, applyMigrations)
}

// MigrateSync is synchronous version of Migrate
func (m *Migrator) MigrateSync(conn driver.Conn, relativeN int) []error {
	pipe := pipep.New()
	go m.Migrate(pipe, conn, relativeN)
	return pipep.ReadErrors(pipe)
}

// Create creates new migration files on disk
func (m *Migrator) Create(incMajor bool, name string, contents ...string) (*file.MigrationFile, error) {
	migrationsPath := m.Path
	files, err := file.ReadMigrationFiles(migrationsPath, m.Driver.FilenameExtension())
	if err != nil {
		return nil, err
	}

	version := file.NewVersion2(0, 0)
	if len(files) > 0 {
		lastFile := files[len(files)-1]
		version = lastFile.Version
	}
	version = version.Inc(incMajor)

	filenamef := "%s_%s.%s.%s"
	name = strings.Replace(name, " ", "_", -1)

	var upContent string
	if len(contents) > 0 {
		upContent = contents[0]
	}
	var downContent string
	if len(contents) > 1 {
		downContent = contents[1]
	}

	minorStr := version.MinorString()
	mfile := &file.MigrationFile{
		Version: version,
		UpFile: &file.File{
			Version:   version,
			FileName:  fmt.Sprintf(filenamef, minorStr, name, "up", m.Driver.FilenameExtension()),
			Name:      name,
			Content:   []byte(upContent),
			Direction: direction.Up,
		},
		DownFile: &file.File{
			Version:   version,
			FileName:  fmt.Sprintf(filenamef, minorStr, name, "down", m.Driver.FilenameExtension()),
			Name:      name,
			Content:   []byte(downContent),
			Direction: direction.Down,
		},
	}

	if err := mfile.WriteFiles(migrationsPath); err != nil {
		return nil, err
	}

	return mfile, nil
}

// MigrateFiles applies migrations in given files
func (m *Migrator) MigrateFiles(pipe chan interface{}, conn driver.Conn, prevFiles, files file.MigrationFiles, applyMigrations file.Migrations) {
	err := m.migrateFiles(pipe, conn, prevFiles, files, applyMigrations)
	go pipep.Close(pipe, err)
}

func (m *Migrator) migrateFiles(pipe chan interface{}, conn driver.Conn, prevFiles, files file.MigrationFiles, applyMigrations file.Migrations) error {
	var (
		d           = m.Driver
		tx          driver.Tx
		err         error
		prevVersion file.Version
	)

	commit := func() error {
		// commit transaction
		err := tx.Commit()
		tx = nil
		return err
	}

	updateFiles := func(stopAt file.Version) (err error) {
		tx, err = conn.Begin()
		if err != nil {
			return err
		}

		sort.Sort(files) // ensure sorted ascending
		for _, mf := range files {
			if mf.Compare(stopAt) >= 0 {
				break
			}
			{ // make copy of file for console output
				f := *mf.UpFile
				f.Direction = 0 // change console output
				pipe <- &f
			}
			// update file contents
			f := mf.Migration(direction.Up)
			pipe1 := pipep.New()
			go d.UpdateFiles(tx, &f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, m.handleInterrupts()); !ok {
				return tx.Rollback()
			}
		}
		return commit()
	}

	if len(applyMigrations) == 0 { // no migrations to apply
		// Write all files if first file doesn't have content.
		// This is here for previous versions that didn't store
		// the file content in the database.
		// This could run even when the first file has content,
		// but that seems wasteful. The next block should be less
		// wasteful since it only update the files when there's
		// a new version.
		if len(prevFiles) > 0 {
			sort.Sort(prevFiles) // ensure sorted ascending
			first := prevFiles[0].UpFile
			if err := first.ReadContent(); err != nil {
				return err
			}
			if len(first.Content) == 0 {
				return updateFiles(files.LastVersion().Inc(true))
			}
		}
		// no migrations to apply
		return nil
	}

	// In case the file content on disk has changed, such as
	// fixing a down file, on up migrations ensure previous
	// migration content matches content on disk.
	first := applyMigrations[0]
	if first.Up() {
		if err := updateFiles(first.Version); err != nil {
			return err
		}
	}

	txPerFile := m.TxPerFile
	for _, f := range applyMigrations {
		// fmt.Println("f", f)
		// commit if per file or major version changed
		if tx != nil && (txPerFile || prevVersion.Major() != f.Major()) {
			if err := commit(); err != nil {
				return err
			}
		}
		// begin new transaction if no active transaction
		if tx == nil {
			tx, err = conn.Begin()
			if err != nil {
				return err
			}
		}

		pipe1 := pipep.New()
		go d.Migrate(tx, &f, pipe1)
		if ok := pipep.WaitAndRedirect(pipe1, pipe, m.handleInterrupts()); !ok {
			return tx.Rollback()
		}

		prevVersion = f.Version
	}
	// commit last transaction
	return commit()
}

// NewPipe is a convenience function for pipe.New().
// This is helpful if the user just wants to import this package and nothing else.
func NewPipe() chan interface{} {
	return pipep.New()
}

// interrupts returns a signal channel if interrupts checking is
// enabled. nil otherwise.
func (m *Migrator) handleInterrupts() chan os.Signal {
	if m.Interrupts {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		return c
	}
	return nil
}

func (m *Migrator) Version(conn driver.Conn) (version file.Version, err error) {
	return m.Driver.Version(conn)
}

// SchemaDir is the dir used to store schema migrations in dump files
const SchemaDir = "schema/"

// DumpSync is synchronous version of Dump
func (m *Migrator) DumpSync(conn driver.CopyConn, dw file.DumpWriter) []error {
	pipe := pipep.New()
	go m.Dump(pipe, conn, dw)
	return pipep.ReadErrors(pipe)
}
func (m *Migrator) Dump(pipe chan interface{}, conn driver.CopyConn, dw file.DumpWriter) {
	var err error
	defer func() {
		go pipep.Close(pipe, err)
	}()

	dd, ok := m.Driver.(driver.DumpDriver)
	if !ok {
		err = errors.New("Driver must be a DumpDriver")
		return
	}

	// get previous files
	prevFiles, err := m.Driver.GetMigrationFiles(conn)
	if err != nil {
		return
	}

	// write schema files
	getWriter := func(dir, name string) (io.WriteCloser, error) {
		// insert 'schema' dir into path
		return dw.Writer(path.Join(SchemaDir, dir), name)
	}
	for _, f := range prevFiles {
		err = f.WriteFileContents(getWriter, true)
		if err != nil {
			return
		}
	}

	// write table data
	pipe1 := pipep.New()
	go dd.Dump(conn, dw, m.Schema, pipe1, m.handleInterrupts)
	if ok := pipep.WaitAndRedirect(pipe1, pipe, m.handleInterrupts()); !ok {
		return
	}
}

// RestoreSync is synchronous version of Restore
func (m *Migrator) RestoreSync(conn driver.CopyConn, dr file.DumpReader) []error {
	pipe := pipep.New()
	go m.Restore(pipe, conn, dr)
	return pipep.ReadErrors(pipe)
}
func (m *Migrator) Restore(pipe chan interface{}, conn driver.CopyConn, dr file.DumpReader) {
	var err error
	defer func() {
		go pipep.Close(pipe, err)
	}()

	dd, ok := m.Driver.(driver.DumpDriver)
	if !ok {
		err = errors.New("Driver must be a DumpDriver")
		return
	}

	schema := m.Schema
	if schema == "" {
		schema = "public"
	}

	if m.Force {
		if err = dd.DeleteSchema(conn, schema); err != nil {
			return
		}
	}
	if err = dd.EnsureVersionTable(conn, schema); err != nil {
		return
	}

	{ // migrate up using schema read from DumpReader
		var openers file.Openers
		openers, err = dr.Files(SchemaDir)
		if err != nil {
			return
		}
		var files file.MigrationFiles
		files, err = file.GetMigrationFiles(openers, m.Driver.FilenameExtension())
		if err != nil {
			return
		}
		if len(files) == 0 {
			err = errors.New("Missing migration files")
			return
		}
		pipe1 := pipep.New()
		go m.up(pipe1, conn, nil, files, file.NewVersion2(0, 0))
		if ok := pipep.WaitAndRedirect(pipe1, pipe, m.handleInterrupts()); !ok {
			return
		}
	}

	if err = dd.TruncateTables(conn, schema); err != nil {
		return
	}

	{ // restore data
		pipe1 := pipep.New()
		go dd.Restore(conn, dr, schema, pipe1, m.handleInterrupts)
		if ok := pipep.WaitAndRedirect(pipe1, pipe, m.handleInterrupts()); !ok {
			return
		}
	}
}
