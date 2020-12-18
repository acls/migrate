package pgx

import (
	"testing"

	"github.com/acls/migrate/file"
	"github.com/acls/migrate/migrate/direction"
	pipep "github.com/acls/migrate/pipe"
	"github.com/acls/migrate/testutil"
)

var schema = "migrate_driver_pgx"

// TestMigrate runs some additional tests on Migrate().
// Basic testing is already done in migrate/migrate_test.go
func TestMigrate(t *testing.T) {
	file.V2 = true

	conn := Conn(testutil.MustInitPgx(t, schema))
	defer conn.Close()

	d := New(schema, "")
	if err := d.EnsureVersionTable(conn); err != nil {
		t.Fatal(err)
	}

	files := file.MigrationFiles{
		file.MigrationFile{
			Version: file.NewVersion2(0, 1),
			UpFile: &file.File{
				FileName:  "001_foobar.up.sql",
				Version:   file.NewVersion2(0, 1),
				Name:      "foobar",
				Direction: direction.Up,
				Content: []byte(`
					CREATE TABLE yolo (
						id serial not null primary key
					);
				`),
			},
			DownFile: &file.File{
				FileName:  "001_foobar.down.sql",
				Version:   file.NewVersion2(0, 1),
				Name:      "foobar",
				Direction: direction.Down,
				Content: []byte(`
					DROP TABLE yolo;
				`),
			},
		},

		file.MigrationFile{
			Version: file.NewVersion2(0, 2),
			UpFile: &file.File{
				FileName:  "002_erroar.up.sql",
				Version:   file.NewVersion2(0, 2),
				Name:      "erroar",
				Direction: direction.Up,
				Content: []byte(`
					CREATE TABLE error (
						id THIS WILL CAUSE AN ERROAR
					)
				`),
			},
			DownFile: &file.File{
				FileName:  "002_erroar.up.sql",
				Version:   file.NewVersion2(0, 2),
				Name:      "erroar",
				Direction: direction.Up,
				Content: []byte(`
					MOAR ERROAR!
				`),
			},
		},
	}

	pipe := pipep.New()
	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mf := files[0].Migration(direction.Up)
	go d.Migrate(tx, &mf, pipe)
	errs := pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	pipe = pipep.New()
	tx, err = conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mf = files[0].Migration(direction.Down)
	go d.Migrate(tx, &mf, pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	pipe = pipep.New()
	tx, err = conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mf = files[1].Migration(direction.Up)
	go d.Migrate(tx, &mf, pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) == 0 {
		t.Error("Expected test case to fail")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
