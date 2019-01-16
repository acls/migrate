// Package file contains functions for low-level migration files handling.
package file

import (
	"bytes"
	"errors"
	"fmt"
	"go/token"
	"io"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/acls/migrate/migrate/direction"
)

// V2 set to true to use version 2 for schema migrations which enables major versions.
// V2 is not backwards compatible with previous version.
// So don't set this to true and then set it to false.
var V2 bool

// File represents one file on disk.
// Example: 001_initial_plan_to_do_sth.up.sql
type File struct {
	Open func() (io.ReadCloser, error)

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

type Version interface {
	Inc(major bool) Version
	String() string
	Major() uint64
	Minor() uint64
	MajorString() string
	MinorString() string
	Compare(other Version) int
}

// Parse parses the version
func ParseVersion(s string) (Version, error) {
	var err error
	var v version
	if !V2 {
		v.major = 0
		v.minor, err = strconv.ParseUint(s, 10, 64)
		return &v, err
	}

	ss := strings.Split(s, "/")
	if len(ss) != 2 {
		return nil, errors.New("Invalid version string (major/minor)")
	}
	if v.major, err = strconv.ParseUint(ss[0], 10, 64); err != nil {
		return nil, errors.New("Invalid major version")
	}
	if v.minor, err = strconv.ParseUint(ss[1], 10, 64); err != nil {
		return nil, errors.New("Invalid minor version")
	}
	return &v, nil
}

func NewVersion(version uint64) Version {
	return NewVersion2(0, version)
}

func NewVersion2(major, minor uint64) Version {
	if !V2 {
		major = 0
	}
	return &version{
		major: major,
		minor: minor,
	}
}

// version of the migration
type version struct {
	major uint64
	minor uint64
}

// Inc increments major or minor
func (v *version) Inc(major bool) Version {
	cv := *v // copy
	if major {
		cv.minor = 1
		cv.major++
	} else {
		cv.minor++
	}
	return &cv
}

func (v version) String() string {
	if !V2 {
		return v.MinorString()
	}
	return fmt.Sprintf("%s/%s", v.MajorString(), v.MinorString())
}

func (v version) Major() uint64 {
	return v.major
}

func (v version) Minor() uint64 {
	return v.minor
}

func (v version) MajorString() string {
	return padLeft(strconv.FormatUint(v.major, 10), "0", 3)
}

func (v version) MinorString() string {
	return padLeft(strconv.FormatUint(v.minor, 10), "0", 4)
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
func (v version) Compare(other Version) int {
	if v.major < other.Major() || (v.major == other.Major() && v.minor < other.Minor()) {
		return -1
	}
	if v.major > other.Major() || (v.major == other.Major() && v.minor > other.Minor()) {
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

func (mf MigrationFile) WriteFiles(baseDir string) (err error) {
	if err = mf.UpFile.Write(baseDir, true); err != nil {
		return
	}
	return mf.DownFile.Write(baseDir, false)
}
func (mf MigrationFile) WriteFileContents(getWriter func(string, string) (io.WriteCloser, error), release bool) (err error) {
	if err = mf.UpFile.WriteContent(getWriter, release); err != nil {
		return
	}
	return mf.DownFile.WriteContent(getWriter, release)
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
func (mf MigrationFiles) LastVersion() Version {
	l := len(mf)
	if l > 0 {
		return mf[l-1].Version
	}
	return NewVersion2(0, 0)
}

// ReadContent reads the file's content if the content is nil
func (f *File) ReadContent() error {
	if f.Content == nil {
		if f.Open == nil {
			return errors.New("File.Open is nil")
		}
		r, err := f.Open()
		if err != nil {
			return err
		}
		defer r.Close()
		content, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}
		f.Content = content
	}
	return nil
}

func (f *File) prevPath(prevDir string) string {
	if !V2 {
		return prevDir
	}
	if f.Version == nil {
		panic("f.Version is nil")
	}
	v := f.Version
	majorStr := v.MajorString()
	if prevDir == "" {
		return majorStr
	}
	return path.Join(prevDir, majorStr)
}

// Write reads the file's content and writes to the passed in path
func (f *File) Write(baseDir string, mkDir bool) (err error) {
	if f == nil {
		return errors.New("File is nil")
	}
	return f.WriteContent(func(dir, name string) (io.WriteCloser, error) {
		dir = path.Join(baseDir, dir)
		// if mkDir {
		_ = os.MkdirAll(dir, 0700)
		// }
		return os.OpenFile(path.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	}, false)
}

// WriteContent reads the file's content and writes to the writer
func (f *File) WriteContent(getWriter func(majorDir string, name string) (io.WriteCloser, error), release bool) (err error) {
	if f == nil {
		return errors.New("File is nil")
	}
	// read
	if err = f.ReadContent(); err != nil {
		return
	}
	majorStr := f.prevPath("")
	file, err := getWriter(majorStr, f.FileName)
	if err != nil {
		return
	}
	defer file.Close()
	// write
	_, err = file.Write(f.Content)
	// release bytes
	if release {
		f.Content = nil
	}
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
		migrations = mf.ToLastFrom(curVersion)
		return
	}
	// wasn't up, so migrate down using previous migrations
	migrations = prevFiles.DownTo(dstVersion)
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
func (mf MigrationFiles) DownTo(dstVersion Version) Migrations {
	sort.Sort(sort.Reverse(mf))
	migrations := make(Migrations, 0)
	for _, migrationFile := range mf {
		if migrationFile.Compare(dstVersion) <= 0 {
			break
		}
		migrations = append(migrations, migrationFile.Migration(direction.Down))
	}
	return migrations
}

// ToFirstFrom fetches all (down) migration files including the migration file
// of the current version to the very first migration file.
func (mf MigrationFiles) ToFirstFrom(version Version) Migrations {
	sort.Sort(sort.Reverse(mf))
	migrations := make(Migrations, 0)
	for _, migrationFile := range mf {
		if migrationFile.Compare(version) <= 0 {
			migrations = append(migrations, migrationFile.Migration(direction.Down))
		}
	}
	return migrations
}

// ToLastFrom fetches all (up) migration files to the most recent migration file.
// The migration file of the current version is not included.
func (mf MigrationFiles) ToLastFrom(version Version) Migrations {
	sort.Sort(mf)
	migrations := make(Migrations, 0)
	for _, migrationFile := range mf {
		if migrationFile.Compare(version) > 0 {
			migrations = append(migrations, migrationFile.Migration(direction.Up))
		}
	}
	return migrations
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
func (mf MigrationFiles) From(version Version, relativeN int) Migrations {
	var d direction.Direction
	if relativeN > 0 {
		d = direction.Up
	} else if relativeN < 0 {
		d = direction.Down
	} else { // relativeN == 0
		return nil
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
	return migrations
}

func (mf MigrationFiles) MissingVersion() Version {
	if len(mf) == 0 {
		return nil
	}

	expected := NewVersion2(0, 1)
	for i := range mf {
		if mf[i].Compare(expected) != 0 {
			if V2 && i != 0 {
				expected = expected.Inc(true)
			}
			if mf[i].Compare(expected) != 0 {
				return expected
			}
		}
		expected = expected.Inc(false)
	}
	return nil
}

// ReadFilesBetween reads the previous and current files and returns the files needed to go from the previous version to the current version
func ReadFilesBetween(prevBasePath, basePath string, filenameExtension string, force bool) (curVersion, dstVersion Version, migrations Migrations, err error) {
	if prevBasePath == "" {
		err = errors.New("Empty prevBasePath")
		return
	}

	var prevFiles MigrationFiles
	// only read files if prev path exists
	if _, e := os.Stat(prevBasePath); !os.IsNotExist(e) {
		prevFiles, err = ReadMigrationFiles(prevBasePath, filenameExtension)
		if err != nil {
			return
		}
	}

	curFiles, err := ReadMigrationFiles(basePath, filenameExtension)
	if err != nil {
		return
	}

	return curFiles.Between(prevFiles, force)
}

// ReadMigrationFiles reads all migration files from a given path
func ReadMigrationFiles(basePath string, filenameExtension string) (files MigrationFiles, err error) {
	openers, err := (&DirReader{BaseDir: basePath}).Files("")
	if err != nil {
		return
	}
	return GetMigrationFiles(openers, filenameExtension)
}
func GetMigrationFiles(openers Openers, filenameExtension string) (files MigrationFiles, err error) {
	tmpFileMap := make(map[string]*MigrationFile)
	for _, ioFile := range openers {
		majorVersion, minorVersion, name, d, err := parseFilenameSchema(V2, ioFile.Name, filenameExtension)
		if err != nil {
			continue
		}
		version := NewVersion2(majorVersion, minorVersion)
		migrationFile, ok := tmpFileMap[version.String()]
		if !ok {
			migrationFile = &MigrationFile{
				Version: version,
			}
			tmpFileMap[version.String()] = migrationFile
		}

		_, filename := path.Split(ioFile.Name)
		file := &File{
			Open:      ioFile.Open,
			FileName:  filename,
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

	files = make(MigrationFiles, 0, len(tmpFileMap))
	for _, file := range tmpFileMap {
		files = append(files, *file)
	}

	sort.Sort(files)
	return files, nil
}

const filenameRegexSuffix = `(?P<minor>[0-9]+)_(?P<name>.*)\.(?P<direction>up|down)\.(?P<ext>.*)$`

var filenameRegex = regexp.MustCompile("^" + filenameRegexSuffix)
var filenameRegexV2 = regexp.MustCompile("^(?P<major>[0-9]+)/" + filenameRegexSuffix)

// parseFilenameSchema parses the filename
func parseFilenameSchema(isV2 bool, filename string, filenameExtension string) (major, version uint64, name string, d direction.Direction, err error) {
	regx := filenameRegex
	if isV2 {
		regx = filenameRegexV2
	}

	matches := regx.FindStringSubmatch(filename)
	if matches == nil {
		err = errors.New("Unable to parse filename schema")
		return
	}
	nameIndices := make(map[string]int)
	for i, name := range regx.SubexpNames() {
		if i != 0 && name != "" {
			nameIndices[name] = i
		}
	}

	if isV2 {
		major, err = strconv.ParseUint(matches[nameIndices["major"]], 10, 0)
		if err != nil {
			err = fmt.Errorf("Unable to parse major version in filename schema: '%v'", matches[0])
			return
		}
	}

	version, err = strconv.ParseUint(matches[nameIndices["minor"]], 10, 0)
	if err != nil {
		err = fmt.Errorf("Unable to parse version in filename schema: '%v'", matches[0])
		return
	}

	name = matches[nameIndices["name"]]

	switch matches[nameIndices["direction"]] {
	case "up":
		d = direction.Up
	case "down":
		d = direction.Down
	default:
		err = fmt.Errorf("Unable to parse up|down in filename schema: '%v'", matches[0])
	}

	if matches[nameIndices["ext"]] != filenameExtension {
		err = fmt.Errorf("Invalid extension in filename schema: '%v'", matches[0])
	}

	return
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
		lineCounter++
	}

	return bytes.Join(newLines, []byte("\n"))
}
