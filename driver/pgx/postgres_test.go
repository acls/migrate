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

	d := New("")
	if err := d.EnsureVersionTable(conn, schema); err != nil {
		t.Fatal(err)
	}

	files := []*file.File{
		{
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
		{
			FileName:  "002_foobar.down.sql",
			Version:   file.NewVersion2(0, 2),
			Name:      "foobar",
			Direction: direction.Down,
			Content: []byte(`
				DROP TABLE yolo;
			`),
		},
		{
			FileName:  "002_foobar.up.sql",
			Version:   file.NewVersion2(0, 2),
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
				CREATE TABLE error (
					id THIS WILL CAUSE AN ERROR
				)
			`),
		},
	}

	pipe := pipep.New()
	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	go d.Migrate(tx, files[0], pipe)
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
	go d.Migrate(tx, files[1], pipe)
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
	go d.Migrate(tx, files[2], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) == 0 {
		t.Error("Expected test case to fail")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
