package file

import (
	"archive/zip"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// zipWriter creates a zip file writer that buffers to disk
type zipWriter struct {
	zw  *zip.Writer
	f   *os.File
	tmp string
	tw  *tmpWriter
}

// NewZipWriter returns a new DumpWriter
func NewZipWriter(zipFile, tmpFile string) (DumpWriter, error) {
	f, err := os.Create(zipFile)
	if err != nil {
		return nil, err
	}
	return &zipWriter{
		zw:  zip.NewWriter(f),
		f:   f,
		tmp: tmpFile,
		tw:  nil,
	}, nil
}

// Close closes the open writer, if one exists, then closes the zip.Writer
func (z *zipWriter) Close() error {
	// close temp writer, if there is one
	if z.tw != nil {
		_ = z.tw.Close() // don't care about result error
	}
	// close zip writer
	_ = z.zw.Close() // don't care about result error
	// close zip file
	return z.f.Close()
}

// Writer creates a tmp file to write to then writes that file to the zip.Writer
func (z *zipWriter) Writer(dir, name string) (io.WriteCloser, error) {
	if z.tw != nil {
		return nil, errors.New("Only one writer can open at a time")
	}

	tmpFile := z.tmp

	f, err := os.Create(tmpFile)
	if err != nil {
		return nil, err
	}
	tw := &tmpWriter{f: f}
	z.tw = tw
	tw.onClose = func() error {
		defer f.Close()

		if z.tw != tw {
			return errors.New("Invalid tmpWriter")
		}
		z.tw = nil // clear tmpWriter

		// ensure it's written to disk
		if err := f.Sync(); err != nil {
			return err
		}

		// seek to the beginning
		offset, err := f.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		if offset != 0 {
			return errors.New("Bad offset?")
		}

		return zipFile(z.zw, path.Join(dir, name), f)
	}
	return tw, nil
}

// zipFile adds a file to a zip.Writer
func zipFile(w *zip.Writer, relPath string, f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}
	header.Name = relPath
	header.Method = zip.Deflate
	writer, err := w.CreateHeader(header)
	if err != nil {
		return err
	}

	_, err = io.CopyN(writer, f, int64(header.UncompressedSize64))
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

type tmpWriter struct {
	f       *os.File
	onClose func() error
}

func (tw *tmpWriter) Write(b []byte) (int, error) {
	return tw.f.Write(b)
}
func (tw *tmpWriter) Close() (err error) {
	return tw.onClose()
}

// zipReader creates a zip file writer that buffers to disk
type zipReader struct {
	zr *zip.Reader
	f  *os.File
}

// NewZipReader returns a new DumpReader
func NewZipReader(zipFile string) (DumpReader, error) {
	f, err := os.Open(zipFile)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	zr, err := zip.NewReader(f, fi.Size())
	if err != nil {
		return nil, err
	}
	return &zipReader{
		zr: zr,
		f:  f,
	}, err
}

func (z *zipReader) Files(dir string) (openers Openers, err error) {
	var name string
	for _, f := range z.zr.File {
		if f.FileInfo().IsDir() || !strings.HasPrefix(f.Name, dir) {
			continue
		}
		if name, err = filepath.Rel(dir, f.Name); err != nil {
			return
		}
		o := Opener{
			Name: name,
			Open: f.Open,
		}
		openers = append(openers, o)
	}
	return
}
func (z *zipReader) Close() error {
	return z.f.Close() // close file
}
