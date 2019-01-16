// Package migrate is imported by other Go code.
// It is the entry point to all migration functions.
package migrate

import (
	"fmt"
	"io/ioutil"
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
	// Path for storing executed migrations that are used for validation and for downgrading when the versions don't exist in MigrationsPath
	PrevPath string
	// True if a transaction should be used for each file instead of per each major version
	TxPerFile bool
	// True if the migration should be interruptable
	Interrupts bool
	// Don't validate base upfiles
	Force bool
}

func (m *Migrator) init(conn driver.Conn, validate bool) (prevFiles, files file.MigrationFiles, version file.Version, err error) {
	if err = m.Driver.EnsureVersionTable(conn); err != nil {
		return
	}

	// only read files if prev path exists
	if m.PrevPath != "" {
		if _, e := os.Stat(m.PrevPath); !os.IsNotExist(e) {
			prevFiles, err = file.ReadMigrationFiles(m.PrevPath, file.FilenameRegex(m.Driver.FilenameExtension()))
			if err != nil {
				return
			}
		}
	}
	files, err = file.ReadMigrationFiles(m.Path, file.FilenameRegex(m.Driver.FilenameExtension()))
	if err != nil {
		return
	}
	version, err = m.Driver.Version(conn)
	if err != nil {
		return
	}

	if validate {
		lastVersion := files.LastVersion()
		if lastVersion.Compare(version) < 0 {
			err = fmt.Errorf("Last file version %v is less than database version %v", lastVersion, version)
			return
		}

		if !m.Force {
			// check that base upfiles match
			if err = files.ValidateBaseFiles(prevFiles); err != nil {
				return
			}
		}
	}
	return
}

// Up applies all available migrations
func (m *Migrator) Up(pipe chan interface{}, conn driver.Conn) {
	_, files, version, err := m.init(conn, true)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrations, err := files.ToLastFrom(version)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	m.MigrateFiles(pipe, conn, files, applyMigrations)
}

// UpSync is synchronous version of Up
func (m *Migrator) UpSync(conn driver.Conn) (errs []error) {
	pipe := pipep.New()
	go m.Up(pipe, conn)
	errs = pipep.ReadErrors(pipe)
	return
}

// Down rolls back all migrations
func (m *Migrator) Down(pipe chan interface{}, conn driver.Conn) {
	_, files, version, err := m.init(conn, true)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrations, err := files.ToFirstFrom(version)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	m.MigrateFiles(pipe, conn, files, applyMigrations)
}

// DownSync is synchronous version of Down
func (m *Migrator) DownSync(conn driver.Conn) (errs []error) {
	pipe := pipep.New()
	go m.Down(pipe, conn)
	errs = pipep.ReadErrors(pipe)
	return
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
func (m *Migrator) RedoSync(conn driver.Conn) (errs []error) {
	pipe := pipep.New()
	go m.Redo(pipe, conn)
	errs = pipep.ReadErrors(pipe)
	return
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
func (m *Migrator) ResetSync(conn driver.Conn) (errs []error) {
	pipe := pipep.New()
	go m.Reset(pipe, conn)
	errs = pipep.ReadErrors(pipe)
	return
}

// MigrateBetween migrates to the destination version
func (m *Migrator) MigrateBetween(pipe chan interface{}, conn driver.Conn) (curVersion, dstVersion file.Version) {
	prevFiles, files, version, err := m.init(conn, false)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	curVersion, dstVersion, applyMigrations, err := files.Between(prevFiles, m.Force)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	if curVersion.Compare(file.Version{}) != 0 &&
		// version.Compare(file.Version{}) != 0 &&
		curVersion.Compare(version) != 0 {
		go pipep.Close(pipe, fmt.Errorf("Database version(%v) doesn't match current migration files version(%v) PrevPath:%s", version, curVersion, m.PrevPath))
		return
	}

	m.MigrateFiles(pipe, conn, files, applyMigrations)
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
	_, files, version, err := m.init(conn, true)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrations, err := files.FromTo(version, dstVersion)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	m.MigrateFiles(pipe, conn, files, applyMigrations)
	return version
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
	_, files, version, err := m.init(conn, true)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrations, err := files.From(version, relativeN)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	if relativeN == 0 {
		applyMigrations = nil
	}

	m.MigrateFiles(pipe, conn, files, applyMigrations)
}

// MigrateSync is synchronous version of Migrate
func (m *Migrator) MigrateSync(conn driver.Conn, relativeN int) (errs []error) {
	pipe := pipep.New()
	go m.Migrate(pipe, conn, relativeN)
	errs = pipep.ReadErrors(pipe)
	return
}

// Create creates new migration files on disk
func (m *Migrator) Create(incMajor bool, name string, contents ...string) (*file.MigrationFile, error) {
	migrationsPath := m.Path
	files, err := file.ReadMigrationFiles(migrationsPath, file.FilenameRegex(m.Driver.FilenameExtension()))
	if err != nil {
		return nil, err
	}

	version := file.Version{}
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

	migrationsPath = path.Join(migrationsPath, version.MajorString())
	os.MkdirAll(migrationsPath, 0700)

	minorStr := version.MinorString()
	mfile := &file.MigrationFile{
		Version: version,
		UpFile: &file.File{
			Path:      migrationsPath,
			FileName:  fmt.Sprintf(filenamef, minorStr, name, "up", m.Driver.FilenameExtension()),
			Name:      name,
			Content:   []byte(upContent),
			Direction: direction.Up,
		},
		DownFile: &file.File{
			Path:      migrationsPath,
			FileName:  fmt.Sprintf(filenamef, minorStr, name, "down", m.Driver.FilenameExtension()),
			Name:      name,
			Content:   []byte(downContent),
			Direction: direction.Down,
		},
	}

	if err := ioutil.WriteFile(path.Join(mfile.UpFile.Path, mfile.UpFile.FileName), mfile.UpFile.Content, 0644); err != nil {
		return nil, err
	}
	if err := ioutil.WriteFile(path.Join(mfile.DownFile.Path, mfile.DownFile.FileName), mfile.DownFile.Content, 0644); err != nil {
		return nil, err
	}

	return mfile, nil
}

// MigrateFiles applies migrations in given files
func (m *Migrator) MigrateFiles(pipe chan interface{}, conn driver.Conn, files file.MigrationFiles, applyMigrations file.Migrations) {
	var err error
	defer func() {
		go pipep.Close(pipe, err)
	}()

	if m.PrevPath == m.Path {
		fmt.Println(m.PrevPath, m.Path)
		err = fmt.Errorf("PrevPath must be different than Path")
		return
	}

	err = m.migrateFiles(pipe, conn, files, applyMigrations)
}

func (m *Migrator) migrateFiles(pipe chan interface{}, conn driver.Conn, files file.MigrationFiles, applyMigrations file.Migrations) error {
	if len(applyMigrations) == 0 {
		return nil
	}

	var (
		commitDir   = m.PrevPath
		txFiles     file.Migrations
		tx          driver.Tx
		err         error
		prevVersion file.Version
	)

	first := applyMigrations[0]
	if commitDir != "" && first.Up() {
		// if migrating up, (re)write prev files that should already exist
		sort.Sort(files)
		for _, f := range files {
			if f.Compare(first.Version) >= 0 {
				break
			}
			f.UpFile.Direction = 0
			pipe <- f.UpFile
			if err := f.WriteFiles(commitDir); err != nil {
				return err
			}
		}
	}

	commit := func() error {
		// commit transaction
		if err := tx.Commit(); err != nil {
			return err
		}
		tx = nil
		// commit(delete/write) files for versions just executed and committed
		if commitDir != "" {
			for _, f := range txFiles {
				if err := f.Commit(commitDir); err != nil {
					return err
				}
			}
		}
		txFiles = nil
		return nil
	}

	txPerFile := m.TxPerFile
	for _, f := range applyMigrations {
		// fmt.Println("f", f)
		// commit if per file or major version changed
		if tx != nil && (txPerFile || prevVersion.Major != f.Major) {
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
		go m.Driver.Migrate(tx, f.File(), pipe1)
		if ok := pipep.WaitAndRedirect(pipe1, pipe, m.handleInterrupts()); !ok {
			return tx.Rollback()
		}

		txFiles = append(txFiles, f)
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
