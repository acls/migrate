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
	conn, driverURL := testutil.MustInitPgx(t, schema)
	defer conn.Close()

	d := New("")
	if err := d.Initialize(driverURL); err != nil {
		t.Fatal(err)
	}

	files := []file.File{
		{
			Path:      "/foobar",
			FileName:  "001_foobar.up.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
				CREATE TABLE yolo (
					id serial not null primary key
				);
			`),
		},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.down.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Down,
			Content: []byte(`
				DROP TABLE yolo;
			`),
		},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.up.sql",
			Version:   2,
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
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	go d.Migrate(tx, files[0], pipe)
	errs := pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	pipe = pipep.New()
	tx, err = d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	go d.Migrate(tx, files[1], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	pipe = pipep.New()
	tx, err = d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	go d.Migrate(tx, files[2], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) == 0 {
		t.Error("Expected test case to fail")
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
