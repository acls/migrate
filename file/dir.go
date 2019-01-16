package file

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
)

// DumpWriter interface
type DumpWriter interface {
	Writer(dir, name string) (io.WriteCloser, error)
	Close() error
}

// DumpReader interface
type DumpReader interface {
	Files(dir string) (Openers, error)
}

// Openers is a list of openers
type Openers []Opener

// Opener struct
type Opener struct {
	Name string
	Open func() (io.ReadCloser, error)
}

// TablesDir prefix for DumpWriter/DumpReader
const TablesDir = "tables/"

// DirWriter struct
type DirWriter struct {
	BaseDir string
}

// Writer opens a writer for the passed in file name
func (d *DirWriter) Writer(dir, name string) (io.WriteCloser, error) {
	dir = path.Join(d.BaseDir, dir)
	os.MkdirAll(dir, 0755)
	return os.Create(path.Join(dir, name))
}
func (d *DirWriter) Close() error {
	return nil
}

// DirReader struct
type DirReader struct {
	BaseDir string
	V2      bool
}

// Files returns  opens a writer for the passed in file name
func (d *DirReader) Files(dir string) (Openers, error) {
	dir = path.Join(d.BaseDir, dir)
	openers := make(Openers, 0)
	err := filepath.Walk(dir, func(fpath string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("walking to %s: %v", fpath, err)
		}
		if info.IsDir() {
			return nil
		}

		name, err := filepath.Rel(dir, fpath)
		if err != nil {
			return err
		}

		o := Opener{
			Name: name,
			Open: func() (io.ReadCloser, error) { return os.Open(fpath) },
		}
		openers = append(openers, o)
		return nil
	})
	return openers, err
}

// IsEmpty returns true if the directory is empty
func IsEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err // not empty or there was an error
}

// RemoveContents removes all the files/directories inside the passed in dir
func RemoveContents(dir string) (err error) {
	// get files/directories in dir, like ioutil.ReadDir, but without sorting
	f, err := os.Open(dir)
	if err != nil {
		// nothing to delete since directory doesn't exist
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	fi, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return
	}
	// remove all files/directories in dir
	for _, d := range fi {
		if err = os.RemoveAll(path.Join(dir, d.Name())); err != nil {
			return
		}
	}
	return
}
