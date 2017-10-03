package migrate

import (
	"io/ioutil"
	"testing"
	// Ensure imports for each driver we wish to test

	_ "github.com/acls/migrate/driver/pgx"
	"github.com/acls/migrate/testutil"
)

var schema = "migrate_migrate"

func TestCreate(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	conn, driverURL := testutil.MustInitPgx(t, schema)
	defer conn.Close()
	if _, err := Create(driverURL, tmpdir, "test_migration"); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(driverURL, tmpdir, "another migration"); err != nil {
		t.Fatal(err)
	}

	files, err := ioutil.ReadDir(tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 4 {
		t.Fatal("Expected 2 new files, got", len(files))
	}
	expectFiles := []string{
		"0001_test_migration.up.sql", "0001_test_migration.down.sql",
		"0002_another_migration.up.sql", "0002_another_migration.down.sql",
	}
	foundCounter := 0
	for _, expectFile := range expectFiles {
		for _, file := range files {
			if expectFile == file.Name() {
				foundCounter++
				break
			}
		}
	}
	if foundCounter != len(expectFiles) {
		t.Error("not all expected files have been found")
	}
}

func TestReset(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	conn, driverURL := testutil.MustInitPgx(t, schema)
	defer conn.Close()
	Create(driverURL, tmpdir, "migration1", "CREATE TABLE t1 (id INTEGER PRIMARY KEY);", "DROP TABLE t1;")
	Create(driverURL, tmpdir, "migration2", "CREATE TABLE t2 (id INTEGER PRIMARY KEY);", "DROP TABLE t2;")

	errs, ok := ResetSync(driverURL, tmpdir)
	if !ok {
		t.Fatal(errs)
	}
	version, err := Version(driverURL, tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("Expected version 2, got %v", version)
	}
}

func TestDown(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	conn, driverURL := testutil.MustInitPgx(t, schema)
	defer conn.Close()
	Create(driverURL, tmpdir, "migration1", "CREATE TABLE t1 (id INTEGER PRIMARY KEY);", "DROP TABLE t1;")
	Create(driverURL, tmpdir, "migration2", "CREATE TABLE t2 (id INTEGER PRIMARY KEY);", "DROP TABLE t2;")

	errs, ok := MigrateSync(driverURL, tmpdir, +1)
	if !ok {
		t.Fatal(errs)
	}
	version, err := Version(driverURL, tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("Expected version 1, got %v", version)
	}

	errs, ok = ResetSync(driverURL, tmpdir)
	if !ok {
		t.Fatal(errs)
	}
	version, err = Version(driverURL, tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("Expected version 2, got %v", version)
	}
}

func TestUp(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	conn, driverURL := testutil.MustInitPgx(t, schema)
	defer conn.Close()
	Create(driverURL, tmpdir, "migration1", "CREATE TABLE t1 (id INTEGER PRIMARY KEY);", "DROP TABLE t1;")
	Create(driverURL, tmpdir, "migration2", "CREATE TABLE t2 (id INTEGER PRIMARY KEY);", "DROP TABLE t2;")

	errs, ok := UpSync(driverURL, tmpdir)
	if !ok {
		t.Fatal(errs)
	}
	version, err := Version(driverURL, tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("Expected version 2, got %v", version)
	}
}

func TestRedo(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	conn, driverURL := testutil.MustInitPgx(t, schema)
	defer conn.Close()
	Create(driverURL, tmpdir, "migration1", "CREATE TABLE t1 (id INTEGER PRIMARY KEY);", "DROP TABLE t1;")
	Create(driverURL, tmpdir, "migration2", "CREATE TABLE t2 (id INTEGER PRIMARY KEY);", "DROP TABLE t2;")

	errs, ok := UpSync(driverURL, tmpdir)
	if !ok {
		t.Fatal(errs)
	}
	version, err := Version(driverURL, tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("Expected version 2, got %v", version)
	}

	errs, ok = RedoSync(driverURL, tmpdir)
	if !ok {
		t.Fatal(errs)
	}
	version, err = Version(driverURL, tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("Expected version 2, got %v", version)
	}
}

func TestMigrate(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	conn, driverURL := testutil.MustInitPgx(t, schema)
	defer conn.Close()
	Create(driverURL, tmpdir, "migration1", "CREATE TABLE t1 (id INTEGER PRIMARY KEY);", "DROP TABLE t1;")
	Create(driverURL, tmpdir, "migration2", "CREATE TABLE t2 (id INTEGER PRIMARY KEY);", "DROP TABLE t2;")

	errs, ok := MigrateSync(driverURL, tmpdir, +2)
	if !ok {
		t.Fatal(errs)
	}
	version, err := Version(driverURL, tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("Expected version 2, got %v", version)
	}

	errs, ok = MigrateSync(driverURL, tmpdir, -2)
	if !ok {
		t.Fatal(errs)
	}
	version, err = Version(driverURL, tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if version != 0 {
		t.Fatalf("Expected version 0, got %v", version)
	}

	errs, ok = MigrateSync(driverURL, tmpdir, +1)
	if !ok {
		t.Fatal(errs)
	}
	version, err = Version(driverURL, tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("Expected version 1, got %v", version)
	}
}

func TestMigrate_Up_Bad(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	conn, driverURL := testutil.MustInitPgx(t, schema)
	defer conn.Close()
	Create(driverURL, tmpdir, "migration1", "CREATE TABLE t1 (id INTEGER PRIMARY KEY);", "DROP TABLE t1;")
	Create(driverURL, tmpdir, "migration2", "Not valid sql", "DROP TABLE t2;")

	_, ok := MigrateSync(driverURL, tmpdir, +2)
	if ok {
		t.Fatal("Expect an error")
	}
	version, err := Version(driverURL, tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("Expected version 1, got %v", version)
	}
}
