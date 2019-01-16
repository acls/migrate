package migrate

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	// Ensure imports for each driver we wish to test

	"github.com/acls/migrate/driver"
	mpgx "github.com/acls/migrate/driver/pgx"
	"github.com/acls/migrate/file"
	"github.com/acls/migrate/testutil"
	"github.com/jackc/pgx"
)

func init() {
	file.V2 = true
}

var schema = "migrate_migrate"

func NewMigratorAndConn(t *testing.T, tmpdir string) (*Migrator, driver.Conn, func()) {
	m := &Migrator{
		Driver:   mpgx.New(""),
		Path:     tmpdir,
		PrevPath: tmpdir + "-prev",
		Schema:   schema,
	}
	return m, mpgx.Conn(testutil.MustInitPgx(t, schema)), func() {
		// cleanup
		os.RemoveAll(m.Path)
		os.RemoveAll(m.PrevPath)
	}
}

func TestCreate(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-Create")
	if err != nil {
		t.Fatal(err)
	}

	m, conn, cleanup := NewMigratorAndConn(t, tmpdir)
	defer cleanup()
	conn.Close()
	if _, err := m.Create(false, "test_migration"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(false, "another migration"); err != nil {
		t.Fatal(err)
	}

	files, err := ioutil.ReadDir(path.Join(tmpdir, file.NewVersion2(0, 0).MajorString()))
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
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-Reset")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	m, conn, cleanup := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	defer cleanup()
	createMigrations(t, m)

	errs := m.ResetSync(conn)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err := m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.NewVersion2(1, 1)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}

func TestDown(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	m, conn, cleanup := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	defer cleanup()
	createMigrations(t, m)

	errs := m.MigrateSync(conn, +1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err := m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.NewVersion2(0, 1)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}

	errs = m.ResetSync(conn)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err = m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect = file.NewVersion2(1, 1)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}

func TestUp(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-Up")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	m, conn, cleanup := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	defer cleanup()
	createMigrations(t, m)

	errs := m.UpSync(conn)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err := m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.NewVersion2(1, 1)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}

func TestRedo(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-Redo")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	m, conn, cleanup := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	defer cleanup()
	createMigrations(t, m)

	errs := m.UpSync(conn)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err := m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.NewVersion2(1, 1)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}

	errs = m.RedoSync(conn)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err = m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect = file.NewVersion2(1, 1)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}

func TestMigrate(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-Migrate")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	m, conn, cleanup := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	defer cleanup()
	createMigrations(t, m)

	errs := m.MigrateSync(conn, +2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err := m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.NewVersion2(0, 2)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}

	errs = m.MigrateSync(conn, -2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err = m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect = file.NewVersion2(0, 0)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}

	errs = m.MigrateSync(conn, +1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err = m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect = file.NewVersion2(0, 1)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}

func TestMigrate_Up_Bad(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-Migrate_Up_Bad")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	m, conn, cleanup := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	defer cleanup()
	m.Create(false, "migration1", "CREATE TABLE t1 (id INTEGER PRIMARY KEY);", "DROP TABLE t1;")
	m.Create(false, "migration2", "Not valid sql", "DROP TABLE t2;")

	errs := m.MigrateSync(conn, +2)
	if len(errs) == 0 {
		t.Fatal("Expect an error")
	}
	version, err := m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.NewVersion2(0, 0)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
}

func TestDumpRestore(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-DumpRestore")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	m, conn, cleanup := NewMigratorAndConn(t, tmpdir)
	defer conn.Close()
	defer cleanup()
	createMigrations(t, m)
	if _, err := m.Create(false, "migration5", `
		CREATE TABLE primary_table (id INTEGER PRIMARY KEY);
		CREATE TABLE foreign_table (
			id SERIAL PRIMARY KEY,
			p_id INTEGER NOT NULL REFERENCES primary_table(id)
		);
		INSERT INTO primary_table (id) VALUES(1),(2);
		INSERT INTO foreign_table (p_id) VALUES(1),(2);
	`, `
		DROP TABLE foreign_table;
		DROP TABLE primary_table;
	`); err != nil {
		t.Fatal(err)
	}

	assertRowCount := func(tbl string, mustSucceed bool, expect int) {
		tbl = pgx.Identifier{m.Schema, tbl}.Sanitize()
		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&count); err != nil {
			if !mustSucceed {
				return
			}
			panic(err)
			t.Fatal(err)
		}
		if expect != count {
			t.Fatalf("Expected %s count %v, got %v", tbl, expect, count)
		}
	}
	assertRowCounts := func(mustSucceed bool, primaryCount, foreignCount int) {
		assertRowCount("primary_table", mustSucceed, primaryCount)
		assertRowCount("foreign_table", mustSucceed, foreignCount)
	}
	appendText := func(filepath, txt string) {
		// add a row
		f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Write([]byte(txt)); err != nil {
			t.Fatal(err)
		}
	}

	errs := m.UpSync(conn)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err := m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect := file.NewVersion2(1, 2)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
	assertRowCounts(true, 2, 2)

	// Dump schema
	dumpDir, err := ioutil.TempDir("/tmp", "migrate-DumpRestore_dump")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dumpDir)
	errs = m.DumpSync(conn.(driver.CopyConn), &file.DirWriter{BaseDir: dumpDir})
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// Migrate to a different version
	errs = m.MigrateSync(conn, -2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err = m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect = file.NewVersion2(0, 3)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
	assertRowCounts(false, 0, 0)

	// add a rows to dumped tables
	appendText(path.Join(dumpDir, file.TablesDir, "primary_table"), "3\n")
	appendText(path.Join(dumpDir, file.TablesDir, "foreign_table"), "3	3\n4	3\n")

	// Restore to a different schema
	m.Schema += "2"
	// ensure the schema doesn't exist
	err = m.Driver.(driver.DumpDriver).DeleteSchema(conn, m.Schema)
	if err != nil {
		t.Fatal(err)
	}
	errs = m.RestoreSync(conn.(driver.CopyConn), &file.DirReader{BaseDir: dumpDir})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err = m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect = file.NewVersion2(1, 2)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
	assertRowCounts(true, 3, 4)

	// Restore to the same schema should fail
	m.Schema = schema
	errs = m.RestoreSync(conn.(driver.CopyConn), &file.DirReader{BaseDir: dumpDir})
	if len(errs) == 0 {
		t.Fatal("Expected an error")
	}

	// Force overwrite the same schema
	m.Force = true
	errs = m.RestoreSync(conn.(driver.CopyConn), &file.DirReader{BaseDir: dumpDir})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	version, err = m.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	expect = file.NewVersion2(1, 2)
	if expect.Compare(version) != 0 {
		t.Fatalf("Expected version %v, got %v", expect, version)
	}
	assertRowCounts(true, 3, 4)
}
