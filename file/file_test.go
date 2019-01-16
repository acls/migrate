package file

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/acls/migrate/migrate/direction"
)

func TestParseFilenameSchema(t *testing.T) {
	var tests = []struct {
		filename          string
		filenameExtension string
		expectVersion     uint64
		expectName        string
		expectDirection   direction.Direction
		expectErr         bool
	}{
		{"001_test_file.up.sql", "sql", 1, "test_file", direction.Up, false},
		{"001_test_file.down.sql", "sql", 1, "test_file", direction.Down, false},
		{"10034_test_file.down.sql", "sql", 10034, "test_file", direction.Down, false},
		{"-1_test_file.down.sql", "sql", 0, "", direction.Up, true},
		{"test_file.down.sql", "sql", 0, "", direction.Up, true},
		{"100_test_file.down", "sql", 0, "", direction.Up, true},
		{"100_test_file.sql", "sql", 0, "", direction.Up, true},
		{"100_test_file", "sql", 0, "", direction.Up, true},
		{"test_file", "sql", 0, "", direction.Up, true},
		{"100", "sql", 0, "", direction.Up, true},
		{".sql", "sql", 0, "", direction.Up, true},
		{"up.sql", "sql", 0, "", direction.Up, true},
		{"down.sql", "sql", 0, "", direction.Up, true},
	}

	for _, test := range tests {
		version, name, migrate, err := parseFilenameSchema(test.filename, FilenameRegex(test.filenameExtension))
		if test.expectErr && err == nil {
			t.Fatal("Expected error, but got none.", test)
		}
		if !test.expectErr && err != nil {
			t.Fatal("Did not expect error, but got one:", err, test)
		}
		if err == nil {
			if version != test.expectVersion {
				t.Error("Wrong version number", test)
			}
			if name != test.expectName {
				t.Error("wrong name", test)
			}
			if migrate != test.expectDirection {
				t.Error("wrong migrate", test)
			}
		}
	}
}

func TestXFiles(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "TestLookForMigrationFilesInSearchPath")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	if err := ioutil.WriteFile(path.Join(tmpdir, "nonsense.txt"), nil, 0755); err != nil {
		t.Fatal("Unable to write files in tmpdir", err)
	}

	majorDir := Version{}.MajorString()
	os.Mkdir(path.Join(tmpdir, majorDir), 0700)

	ioutil.WriteFile(path.Join(tmpdir, majorDir, "002_migrationfile.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, majorDir, "002_migrationfile.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, majorDir, "001_migrationfile.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, majorDir, "001_migrationfile.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, majorDir, "101_create_table.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, majorDir, "101_drop_tables.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, majorDir, "301_migrationfile.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, majorDir, "301_migrationfile.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, majorDir, "401_migrationfile.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, majorDir, "401_migrationfile.down.sql"), []byte("test"), 0755)

	majorDir = Version{Major: 1}.MajorString()
	os.Mkdir(path.Join(tmpdir, majorDir), 0700)

	ioutil.WriteFile(path.Join(tmpdir, majorDir, "001_migrationfile.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, majorDir, "001_migrationfile.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, majorDir, "401_migrationfile.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, majorDir, "401_migrationfile.down.sql"), []byte("test"), 0755)

	files, err := ReadMigrationFiles(tmpdir, FilenameRegex("sql"))
	if err != nil {
		t.Fatal(err)
	}

	if len(files) == 0 {
		t.Fatal("No files returned.")
	}

	wantedOrder := []Version{
		{0, 1},
		{0, 2},
		{0, 101},
		{0, 301},
		{0, 401},
		{1, 1},
		{1, 401},
	}

	if len(files) != len(wantedOrder) {
		t.Fatal("Wrong number of files returned.")
	}

	// test sort order
	for i, want := range wantedOrder {
		if files[i].Compare(want) != 0 {
			t.Error("Sort order is incorrect")
			t.Error(files[i])
		}
	}

	// test read
	if err := files[4].DownFile.ReadContent(); err != nil {
		t.Error("Unable to read file", err)
	}
	if files[4].DownFile.Content == nil {
		t.Fatal("Read content is nil")
	}
	if string(files[4].DownFile.Content) != "test" {
		t.Fatal("Read content is wrong")
	}

	// test names
	if files[0].UpFile.Name != "migrationfile" {
		t.Error("file name is not correct", files[0].UpFile.Name)
	}
	if files[0].UpFile.FileName != "001_migrationfile.up.sql" {
		t.Error("file name is not correct", files[0].UpFile.FileName)
	}

	// test file.From()
	var tests = []struct {
		from        Version
		relative    int
		expectRange []Version
	}{
		{Version{0, 0}, 2, []Version{{0, 1}, {0, 2}}},
		{Version{0, 1}, 4, []Version{{0, 2}, {0, 101}, {0, 301}, {0, 401}}},
		{Version{0, 1}, 0, nil},
		{Version{0, 0}, 1, []Version{{0, 1}}},
		{Version{0, 0}, 0, nil},
		{Version{0, 101}, -2, []Version{{0, 101}, {0, 2}}},
		{Version{0, 401}, -1, []Version{{0, 401}}},
	}

	for _, test := range tests {
		rangeFiles, err := files.From(test.from, test.relative)
		if err != nil {
			t.Error("Unable to fetch range:", err)
		}
		if len(rangeFiles) != len(test.expectRange) {
			t.Fatalf("file.From(): expected %v files, got %v. For test %v.", len(test.expectRange), len(rangeFiles), test.expectRange)
		}

		for i, version := range test.expectRange {
			if rangeFiles[i].Version.Compare(version) != 0 {
				t.Fatal("file.From(): returned files dont match expectations", test.expectRange)
			}
		}
	}

	// test ToFirstFrom
	tffFiles, err := files.ToFirstFrom(Version{1, 401})
	if err != nil {
		t.Fatal(err)
	}
	if len(tffFiles) != len(wantedOrder) {
		t.Fatalf("Wrong number of files returned by ToFirstFrom(), expected %v, got %v.", len(wantedOrder), len(tffFiles))
	}
	if tffFiles[0].Up() {
		t.Error("ToFirstFrom() did not return DownFiles")
	}

	// test ToLastFrom
	tofFiles, err := files.ToLastFrom(Version{0, 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(tofFiles) != len(wantedOrder) {
		t.Fatalf("Wrong number of files returned by ToLastFrom(), expected %v, got %v.", len(wantedOrder), len(tofFiles))
	}
	if !tofFiles[0].Up() {
		t.Error("ToFirstFrom() did not return UpFiles")
	}
}

func TestDuplicateFiles(t *testing.T) {
	dups := []string{
		"001_migration.up.sql",
		"001_duplicate.up.sql",
	}

	root, cleanFn, err := makeFiles("TestDuplicateFiles", dups...)
	defer cleanFn()

	if err != nil {
		t.Fatal(err)
	}

	_, err = ReadMigrationFiles(root, FilenameRegex("sql"))
	if err == nil {
		t.Fatal("Expected duplicate migration file error")
	}
}

// makeFiles takes an identifier, and a list of file names and uses them to create a temporary
// directory populated with files named with the names passed in.  makeFiles returns the root
// directory name, and a func suitable for a defer cleanup to remove the temporary files after
// the calling function exits.
func makeFiles(testname string, names ...string) (root string, cleanup func(), err error) {
	cleanup = func() {}
	root, err = ioutil.TempDir("/tmp", testname)
	if err != nil {
		return
	}
	cleanup = func() { os.RemoveAll(root) }
	if err = ioutil.WriteFile(path.Join(root, "nonsense.txt"), nil, 0755); err != nil {
		return
	}

	os.Mkdir(path.Join(root, "000"), 0700)

	for _, name := range names {
		if err = ioutil.WriteFile(path.Join(root, "000", name), nil, 0755); err != nil {
			return
		}
	}
	return
}
