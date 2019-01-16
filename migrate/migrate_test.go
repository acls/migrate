package migrate

import (
	"io/ioutil"
	"path"
	"testing"
	// Ensure imports for each driver we wish to test

	"github.com/acls/migrate/driver"
	mpgx "github.com/acls/migrate/driver/pgx"
	"github.com/acls/migrate/file"
	"github.com/acls/migrate/testutil"
)

var schema = "migrate_migrate"

func NewMigratorAndConn(t *testing.T, tmpdir string) (*Migrator, driver.Conn) {
	return &Migrator{
		Driver:   mpgx.New(""),
		Path:     tmpdir,
		PrevPath: tmpdir + "-prev",
	}, mpgx.Conn(testutil.MustInitPgx(t, schema))
}

func TestCreate(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	m, conn := NewMigratorAndConn(t, tmpdir)
	conn.Close()
	if _, err := m.Create(false, "test_migration"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(false, "another migration"); err != nil {
		t.Fatal(err)
	}

	files, err := ioutil.ReadDir(path.Join(tmpdir, file.Version{}.MajorString()))
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
		t.Error("not all expected files have been found", foundCounter, len(expectFiles))
	}
}

func createMigrations(t *testing.T, m *Migrator) {
	if _, err := m.Create(false, "migration1", "CREATE TABLE t1 (id INTEGER PRIMARY KEY);", "DROP TABLE t1;"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(false, "migration2", "CREATE TABLE t2 (id INTEGER PRIMARY KEY);", "DROP TABLE t2;"); err != nil {
		t.Fatal(err)
	}
	// temp table 'tmp_tbl' will conflict if not in separate transactions
	if _, err := m.Create(false, "migration3", `CREATE TABLE t3 (id INTEGER PRIMARY KEY);
		CREATE TEMP TABLE tmp_tbl ON COMMIT DROP AS SELECT 1;`, "DROP TABLE t3;"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(true, "migration4", `CREATE TABLE t4 (id INTEGER PRIMARY KEY);
		CREATE TEMP TABLE tmp_tbl ON COMMIT DROP AS SELECT 1;`, "DROP TABLE t4;"); err != nil {
		t.Fatal(err)
	}
}

func TestReset(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	m, conn := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	createMigrations(t, m)

	errs := m.ResetSync(conn)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err := m.Driver.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.Version{Major: 1, Minor: 1}
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}

func TestDown(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	m, conn := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	createMigrations(t, m)

	errs := m.MigrateSync(conn, +1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err := m.Driver.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.Version{Major: 0, Minor: 1}
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}

	errs = m.ResetSync(conn)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err = m.Driver.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect = file.Version{Major: 1, Minor: 1}
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}

func TestUp(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	m, conn := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	createMigrations(t, m)

	errs := m.UpSync(conn)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err := m.Driver.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.Version{Major: 1, Minor: 1}
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}

func TestRedo(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	m, conn := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	createMigrations(t, m)

	errs := m.UpSync(conn)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err := m.Driver.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.Version{Major: 1, Minor: 1}
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}

	errs = m.RedoSync(conn)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err = m.Driver.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect = file.Version{Major: 1, Minor: 1}
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}

func TestMigrate(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	m, conn := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	createMigrations(t, m)

	errs := m.MigrateSync(conn, +2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err := m.Driver.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.Version{Major: 0, Minor: 2}
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}

	errs = m.MigrateSync(conn, -2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err = m.Driver.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect = file.Version{Major: 0, Minor: 0}
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}

	errs = m.MigrateSync(conn, +1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err = m.Driver.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect = file.Version{Major: 0, Minor: 1}
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}

func TestMigrate_Up_Bad(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}

	m, conn := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	m.Create(false, "migration1", "CREATE TABLE t1 (id INTEGER PRIMARY KEY);", "DROP TABLE t1;")
	m.Create(false, "migration2", "Not valid sql", "DROP TABLE t2;")

	errs := m.MigrateSync(conn, +2)
	if len(errs) == 0 {
		t.Fatal("Expect an error")
	}
	version, err := m.Driver.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.Version{Major: 0, Minor: 0}
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}
