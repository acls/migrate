// Package file contains functions for low-level migration files handling.
package file

import (
	"bytes"
	"errors"
	"fmt"
	"go/token"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/acls/migrate/migrate/direction"
)

var filenameRegex = `^([0-9]+)_(.*)\.(up|down)\.%s$`

// FilenameRegex builds regular expression stmt with given
// filename extension from driver.
func FilenameRegex(filenameExtension string) *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf(filenameRegex, filenameExtension))
}

// File represents one file on disk.
// Example: 001_initial_plan_to_do_sth.up.sql
type File struct {
	// absolute path to file
	Path string

	// the name of the file
	FileName string

	// version parsed from filename
	Version

	// the actual migration name parsed from filename
	Name string

	// content of the file
	Content []byte

	// UP or DOWN migration
	Direction direction.Direction
}

// Files is a slice of Files
type Files []*File

// Version of the migration
type Version struct {
	Major uint64
	Minor uint64
}

// Inc increments major or minor
func (v Version) Inc(major bool) Version {
	if major {
		v.Minor = 1
		v.Major++
	} else {
		v.Minor++
	}
	return v
}

// Parse parses the version
func (v *Version) Parse(s string) error {
	ss := strings.Split(s, "/")
	if len(ss) != 2 {
		return errors.New("Invalid version string (major/minor)")
	}
	var err error
	if v.Major, err = strconv.ParseUint(ss[0], 10, 64); err != nil {
		return errors.New("Invalid major version")
	}
	if v.Minor, err = strconv.ParseUint(ss[1], 10, 64); err != nil {
		return errors.New("Invalid minor version")
	}
	return nil
}

func (v Version) String() string {
	return fmt.Sprintf("%s/%s", v.MajorString(), v.MinorString())
}

func (v Version) MajorString() string {
	return padLeft(strconv.FormatUint(v.Major, 10), "0", 3)
}

func (v Version) MinorString() string {
	return padLeft(strconv.FormatUint(v.Minor, 10), "0", 4)
}

func padLeft(s, char string, length int) string {
	if len(s)%length != 0 {
		s = strings.Repeat(char, length-len(s)%length) + s
	}
	return s
}

// Compare returns -1 when this is less than passed in.
// Compare returns 1 when this is more than passed in.
// Compare returns 0 when this is equal to passed in.
func (v Version) Compare(v2 Version) int {
	if v.Major < v2.Major || (v.Major == v2.Major && v.Minor < v2.Minor) {
		return -1
	}
	if v.Major > v2.Major || (v.Major == v2.Major && v.Minor > v2.Minor) {
		return 1
	}
	return 0
}

type Migrations []Migration

func (m Migrations) Len() int {
	return len(m)
}
func (m Migrations) Less(i, j int) bool {
	return m[i].Compare(m[j].Version) < 0
}
func (m Migrations) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

type Migration struct {
	Version
	migrationFile MigrationFile
	d             direction.Direction
}

func (m Migration) Up() bool {
	return m.d != direction.Down
}

func (m Migration) File() *File {
	if m.Up() {
		return m.migrationFile.UpFile
	}
	return m.migrationFile.DownFile
}

func (m Migration) Commit(prevDir string) error {
	if m.d == direction.Down {
		return m.migrationFile.DeleteFiles(prevDir)
	}
	return m.migrationFile.WriteFiles(prevDir)
}

// MigrationFile represents both the UP and the DOWN migration file.
type MigrationFile struct {
	// version of the migration file, parsed from the filenames
	Version

	// reference to the *up* migration file
	UpFile *File

	// reference to the *down* migration file
	DownFile *File
}

// Migration returns the migration for the passed in direction
func (mf MigrationFile) Migration(d direction.Direction) (m Migration) {
	m.Version = mf.Version
	m.migrationFile = mf
	m.d = d
	return
}

func (mf MigrationFile) WriteFiles(prevDir string) (err error) {
	if err = mf.UpFile.WriteContent(prevDir, true); err != nil {
		return
	}
	return mf.DownFile.WriteContent(prevDir, false)
}

func (mf MigrationFile) DeleteFiles(prevDir string) (err error) {
	if err = mf.UpFile.Delete(prevDir); err != nil {
		return
	}
	return mf.DownFile.Delete(prevDir)
}

// MigrationFiles is a slice of MigrationFiles
type MigrationFiles []MigrationFile

// LastVersion returns the last version or empty
func (mf MigrationFiles) LastVersion() (v Version) {
	l := len(mf)
	if l > 0 {
		v = mf[l-1].Version
	}
	return
}

// ReadContent reads the file's content if the content is empty
func (f *File) ReadContent() error {
	if len(f.Content) == 0 {
		content, err := ioutil.ReadFile(path.Join(f.Path, f.FileName))
		if err != nil {
			return err
		}
		f.Content = content
	}
	return nil
}

func (f *File) prevPath(prevDir string) string {
	return path.Join(prevDir, f.Version.MajorString())
}

// WriteContent reads the file's content and writes to the passed in path
func (f *File) WriteContent(prevDir string, mkDir bool) (err error) {
	if f == nil {
		return errors.New("File is nil")
	}
	majorDir := f.prevPath(prevDir)
	// read
	if err = f.ReadContent(); err != nil {
		return
	}
	// write
	if mkDir {
		_ = os.MkdirAll(majorDir, 0700)
	}
	file, err := os.Create(path.Join(majorDir, f.FileName))
	if err != nil {
		return
	}
	defer file.Close()
	_, err = file.Write(f.Content)
	return
}

// Delete reads the file's content and writes to the passed in path
func (f *File) Delete(prevDir string) (err error) {
	if f == nil {
		return errors.New("File is nil")
	}
	majorDir := f.prevPath(prevDir)
	// delete
	err = os.Remove(path.Join(majorDir, f.FileName))
	// ignore does not exist errors
	if os.IsNotExist(err) {
		err = nil
	}
	// try to remove dir. errors are expected when dir is not empty.
	_ = os.Remove(majorDir)
	return
}

// Between either returns migrations to migrate down using the previous migrations or it
// returns migrations to migrate up from the end of the previous migrations to the current migrations.
// 'force' should only be used if the text is different, but the end result is the same.
// Such as adding/removing comments or adding 'IF EXISTS'/'IF NOT EXISTS'
func (mf MigrationFiles) Between(prevFiles MigrationFiles, force bool) (curVersion, dstVersion Version, migrations Migrations, err error) {
	if len(mf) == 0 {
		err = fmt.Errorf("No migration files")
		return
	}

	sort.Sort(mf) // ascending

	// current version is taken from previous files
	curVersion = prevFiles.LastVersion()
	// destination version is taken from this
	dstVersion = mf.LastVersion()

	// try to migrate up
	if curVersion.Compare(dstVersion) <= 0 {
		if !force {
			// validate base upfiles are the same
			if err = mf.ValidateBaseFiles(prevFiles); err != nil {
				return
			}
		}
		// migrate up
		migrations, err = mf.ToLastFrom(curVersion)
		return
	}
	// wasn't up, so migrate down using previous migrations
	migrations, err = prevFiles.DownTo(dstVersion)
	return
}

// ValidateBaseFiles validates that the base files have the same versions and upfile content
func (mf MigrationFiles) ValidateBaseFiles(prevFiles MigrationFiles) error {
	if len(mf) < len(prevFiles) {
		return fmt.Errorf("Less migration files than previous migration files")
	}
	// check if current files are contiguous
	if missing := mf.MissingVersion(); missing != nil {
		return fmt.Errorf("Missing version: %d", missing)
	}
	// compare upfiles up to end of previous files
	for i, prev := range prevFiles {
		file := mf[i]
		// compare versions
		if prev.Compare(file.Version) != 0 {
			return fmt.Errorf("Expected version %v, but got %v", prev.Version, file.Version)
		}
		// compare upfile content
		if err := prev.UpFile.ReadContent(); err != nil {
			return fmt.Errorf("Failed to read previous upfile content: %v", err)
		}
		if err := file.UpFile.ReadContent(); err != nil {
			return fmt.Errorf("Failed to read upfile content: %v", err)
		}
		if bytes.Compare(prev.UpFile.Content, file.UpFile.Content) != 0 {
			return fmt.Errorf("Base upfile contents differ for version %v. "+
				"The '-force' flag can be added to bypass this validation. "+
				"Only do so if the text is different, but the schema change is the same. "+
				"E.g.: adding/removing comments", prev.Version)
		}
	}
	return nil
}

// DownTo fetches all (down) migration files including the migration file
// of the current version to the very first migration file.
func (mf MigrationFiles) DownTo(dstVersion Version) (Migrations, error) {
	sort.Sort(sort.Reverse(mf))
	migrations := make(Migrations, 0)
	for _, migrationFile := range mf {
		if migrationFile.Compare(dstVersion) <= 0 {
			break
		}
		migrations = append(migrations, migrationFile.Migration(direction.Down))
	}
	return migrations, nil
}

// ToFirstFrom fetches all (down) migration files including the migration file
// of the current version to the very first migration file.
func (mf MigrationFiles) ToFirstFrom(version Version) (Migrations, error) {
	sort.Sort(sort.Reverse(mf))
	migrations := make(Migrations, 0)
	for _, migrationFile := range mf {
		if migrationFile.Compare(version) <= 0 {
			migrations = append(migrations, migrationFile.Migration(direction.Down))
		}
	}
	return migrations, nil
}

// ToLastFrom fetches all (up) migration files to the most recent migration file.
// The migration file of the current version is not included.
func (mf MigrationFiles) ToLastFrom(version Version) (Migrations, error) {
	sort.Sort(mf)
	migrations := make(Migrations, 0)
	for _, migrationFile := range mf {
		if migrationFile.Compare(version) > 0 {
			migrations = append(migrations, migrationFile.Migration(direction.Up))
		}
	}
	return migrations, nil
}

// FromTo returns the migration files between the two passed in versions
func (mf MigrationFiles) FromTo(startVersion, stopVersion Version) (migrations Migrations, err error) {
	if startVersion.Compare(stopVersion) == 0 {
		return
	}

	d := direction.Up
	if startVersion.Compare(stopVersion) > 0 {
		d = direction.Down
		startVersion, stopVersion = stopVersion, startVersion
	}

	sort.Sort(mf)
	for _, migrationFile := range mf {
		if migrationFile.Compare(startVersion) <= 0 {
			// skip until start version is reached
			continue
		}
		if migrationFile.Compare(stopVersion) > 0 {
			// found destination version
			return
		}
		// add file
		migrations = append(migrations, migrationFile.Migration(d))
	}
	if d == direction.Down {
		// reverse migrations if going down
		sort.Sort(sort.Reverse(migrations))
	}
	return
}

// From travels relatively through migration files.
//
// 		+1 will fetch the next up migration file
// 		+2 will fetch the next two up migration files
// 		+n will fetch ...
// 		-1 will fetch the the previous down migration file
// 		-2 will fetch the next two previous down migration files
//		-n will fetch ...
func (mf MigrationFiles) From(version Version, relativeN int) (Migrations, error) {
	var d direction.Direction
	if relativeN > 0 {
		d = direction.Up
	} else if relativeN < 0 {
		d = direction.Down
	} else { // relativeN == 0
		return nil, nil
	}

	if d == direction.Down {
		sort.Sort(sort.Reverse(mf))
	} else {
		sort.Sort(mf)
	}

	migrations := make(Migrations, 0)

	counter := relativeN
	if relativeN < 0 {
		counter = relativeN * -1
	}

	for _, migrationFile := range mf {
		if counter > 0 {

			if d == direction.Up && migrationFile.Compare(version) > 0 {
				migrations = append(migrations, migrationFile.Migration(direction.Up))
				counter--
			} else if d == direction.Down && migrationFile.Compare(version) <= 0 {
				migrations = append(migrations, migrationFile.Migration(direction.Down))
				counter--
			}
		} else {
			break
		}
	}
	return migrations, nil
}

func (mf MigrationFiles) MissingVersion() *Version {
	expected := Version{Major: 0, Minor: 1}
	for i := range mf {
		if mf[i].Compare(expected) != 0 {
			if i != 0 {
				expected = expected.Inc(true)
			}
			if mf[i].Compare(expected) != 0 {
				return &expected
			}
		}
		expected = expected.Inc(false)
	}
	return nil
}

// ReadFilesBetween reads the previous and current files and returns the files needed to go from the previous version to the current version
func ReadFilesBetween(prevBasePath, basePath string, filenameRegex *regexp.Regexp, force bool) (curVersion, dstVersion Version, migrations Migrations, err error) {
	if prevBasePath == "" {
		err = errors.New("Empty prevBasePath")
		return
	}

	var prevFiles MigrationFiles
	// only read files if prev path exists
	if _, e := os.Stat(prevBasePath); !os.IsNotExist(e) {
		prevFiles, err = ReadMigrationFiles(prevBasePath, filenameRegex)
		if err != nil {
			return
		}
	}

	curFiles, err := ReadMigrationFiles(basePath, filenameRegex)
	if err != nil {
		return
	}

	return curFiles.Between(prevFiles, force)
}

// ReadMigrationFiles reads all migration files from a given path
func ReadMigrationFiles(basePath string, filenameRegex *regexp.Regexp) (files MigrationFiles, err error) {
	dirs, err := ioutil.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		// parse major version
		major, err := strconv.ParseUint(d.Name(), 10, 0)
		if err != nil {
			return nil, err
		}
		minorFiles, err := readMinorFiles(major, path.Join(basePath, d.Name()), filenameRegex)
		if err != nil {
			return nil, err
		}
		files = append(files, minorFiles...)
	}

	sort.Sort(files)
	return files, nil
}
func readMinorFiles(majorVersion uint64, path string, filenameRegex *regexp.Regexp) (files MigrationFiles, err error) {
	// find all migration files in path
	ioFiles, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}
	type tmpFile struct {
		version  Version
		name     string
		filename string
		d        direction.Direction
	}
	tmpFileMap := make(map[Version]*MigrationFile)
	for _, file := range ioFiles {
		minorVersion, name, d, err := parseFilenameSchema(file.Name(), filenameRegex)
		if err == nil {
			version := Version{majorVersion, minorVersion}
			migrationFile, ok := tmpFileMap[version]
			if !ok {
				migrationFile = &MigrationFile{
					Version: version,
				}
				tmpFileMap[version] = migrationFile
			}

			file := &File{
				Path:      path,
				FileName:  file.Name(),
				Version:   version,
				Name:      name,
				Content:   nil,
				Direction: d,
			}
			switch d {
			case direction.Up:
				if migrationFile.UpFile != nil {
					return nil, fmt.Errorf("duplicate migrate up file version %d", version)
				}
				migrationFile.UpFile = file
			case direction.Down:
				if migrationFile.DownFile != nil {
					return nil, fmt.Errorf("duplicate migrate down file version %d", version)
				}
				migrationFile.DownFile = file
			default:
				return nil, errors.New("Unsupported direction.Direction Type")
			}
		}
	}

	files = make(MigrationFiles, 0, len(tmpFileMap))
	for _, file := range tmpFileMap {
		files = append(files, *file)
	}
	return
}

// parseFilenameSchema parses the filename
func parseFilenameSchema(filename string, filenameRegex *regexp.Regexp) (version uint64, name string, d direction.Direction, err error) {
	matches := filenameRegex.FindStringSubmatch(filename)
	if len(matches) != 4 {
		return 0, "", 0, errors.New("Unable to parse filename schema")
	}

	version, err = strconv.ParseUint(matches[1], 10, 0)
	if err != nil {
		return 0, "", 0, fmt.Errorf("Unable to parse version '%v' in filename schema", matches[0])
	}

	if matches[3] == "up" {
		d = direction.Up
	} else if matches[3] == "down" {
		d = direction.Down
	} else {
		return 0, "", 0, fmt.Errorf("Unable to parse up|down '%v' in filename schema", matches[3])
	}

	return version, matches[2], d, nil
}

// Len is the number of elements in the collection.
// Required by Sort Interface{}
func (mf MigrationFiles) Len() int {
	return len(mf)
}

// Less reports whether the element with
// index i should sort before the element with index j.
// Required by Sort Interface{}
func (mf MigrationFiles) Less(i, j int) bool {
	return mf[i].Compare(mf[j].Version) < 0
}

// Swap swaps the elements with indexes i and j.
// Required by Sort Interface{}
func (mf MigrationFiles) Swap(i, j int) {
	mf[i], mf[j] = mf[j], mf[i]
}

// LineColumnFromOffset reads data and returns line and column integer
// for a given offset.
func LineColumnFromOffset(data []byte, offset int) (line, column int) {
	// TODO is there a better way?
	fs := token.NewFileSet()
	tf := fs.AddFile("", fs.Base(), len(data))
	tf.SetLinesForContent(data)
	pos := tf.Position(tf.Pos(offset))
	return pos.Line, pos.Column
}

// LinesBeforeAndAfter reads n lines before and after a given line.
// Set lineNumbers to true, to prepend line numbers.
func LinesBeforeAndAfter(data []byte, line, before, after int, lineNumbers bool) []byte {
	// TODO(mattes): Trim empty lines at the beginning and at the end
	// TODO(mattes): Trim offset whitespace at the beginning of each line, so that indentation is preserved
	startLine := line - before
	endLine := line + after
	lines := bytes.SplitN(data, []byte("\n"), endLine+1)

	if startLine < 0 {
		startLine = 0
	}
	if endLine > len(lines) {
		endLine = len(lines)
	}

	selectLines := lines[startLine:endLine]
	newLines := make([][]byte, 0)
	lineCounter := startLine + 1
	lineNumberDigits := len(strconv.Itoa(len(selectLines)))
	for _, l := range selectLines {
		lineCounterStr := strconv.Itoa(lineCounter)
		if len(lineCounterStr)%lineNumberDigits != 0 {
			lineCounterStr = strings.Repeat(" ", lineNumberDigits-len(lineCounterStr)%lineNumberDigits) + lineCounterStr
		}

		lNew := l
		if lineNumbers {
			lNew = append([]byte(lineCounterStr+": "), lNew...)
		}
		newLines = append(newLines, lNew)
		lineCounter += 1
	}

	return bytes.Join(newLines, []byte("\n"))
}
