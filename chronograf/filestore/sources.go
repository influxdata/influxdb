package filestore

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"

	"github.com/influxdata/chronograf"
)

// SrcExt is the the file extension searched for in the directory for source files
const SrcExt = ".src"

var _ chronograf.SourcesStore = &Sources{}

// Sources are JSON sources stored in the filesystem
type Sources struct {
	Dir     string                                      // Dir is the directory containing the sources.
	Load    func(string, interface{}) error             // Load loads string name and dashbaord passed in as interface
	Create  func(string, interface{}) error             // Create will write source to file.
	ReadDir func(dirname string) ([]os.FileInfo, error) // ReadDir reads the directory named by dirname and returns a list of directory entries sorted by filename.
	Remove  func(name string) error                     // Remove file
	IDs     chronograf.ID                               // IDs generate unique ids for new sources
	Logger  chronograf.Logger
}

// NewSources constructs a source store wrapping a file system directory
func NewSources(dir string, ids chronograf.ID, logger chronograf.Logger) chronograf.SourcesStore {
	return &Sources{
		Dir:     dir,
		Load:    load,
		Create:  create,
		ReadDir: ioutil.ReadDir,
		Remove:  os.Remove,
		IDs:     ids,
		Logger:  logger,
	}
}

func sourceFile(dir string, source chronograf.Source) string {
	base := fmt.Sprintf("%s%s", source.Name, SrcExt)
	return path.Join(dir, base)
}

// All returns all sources from the directory
func (d *Sources) All(ctx context.Context) ([]chronograf.Source, error) {
	files, err := d.ReadDir(d.Dir)
	if err != nil {
		return nil, err
	}

	sources := []chronograf.Source{}
	for _, file := range files {
		if path.Ext(file.Name()) != SrcExt {
			continue
		}
		var source chronograf.Source
		if err := d.Load(path.Join(d.Dir, file.Name()), &source); err != nil {
			var fmtErr = fmt.Errorf("Error loading source configuration from %v:\n%v", path.Join(d.Dir, file.Name()), err)
			d.Logger.Error(fmtErr)
			continue // We want to load all files we can.
		} else {
			sources = append(sources, source)
		}
	}
	return sources, nil
}

// Add creates a new source within the directory
func (d *Sources) Add(ctx context.Context, source chronograf.Source) (chronograf.Source, error) {
	genID, err := d.IDs.Generate()
	if err != nil {
		d.Logger.
			WithField("component", "source").
			Error("Unable to generate ID")
		return chronograf.Source{}, err
	}

	id, err := strconv.Atoi(genID)
	if err != nil {
		d.Logger.
			WithField("component", "source").
			Error("Unable to convert ID")
		return chronograf.Source{}, err
	}

	source.ID = id

	file := sourceFile(d.Dir, source)
	if err = d.Create(file, source); err != nil {
		if err == chronograf.ErrSourceInvalid {
			d.Logger.
				WithField("component", "source").
				WithField("name", file).
				Error("Invalid Source: ", err)
		} else {
			d.Logger.
				WithField("component", "source").
				WithField("name", file).
				Error("Unable to write source:", err)
		}
		return chronograf.Source{}, err
	}
	return source, nil
}

// Delete removes a source file from the directory
func (d *Sources) Delete(ctx context.Context, source chronograf.Source) error {
	_, file, err := d.idToFile(source.ID)
	if err != nil {
		return err
	}

	if err := d.Remove(file); err != nil {
		d.Logger.
			WithField("component", "source").
			WithField("name", file).
			Error("Unable to remove source:", err)
		return err
	}
	return nil
}

// Get returns a source file from the source directory
func (d *Sources) Get(ctx context.Context, id int) (chronograf.Source, error) {
	board, file, err := d.idToFile(id)
	if err != nil {
		if err == chronograf.ErrSourceNotFound {
			d.Logger.
				WithField("component", "source").
				WithField("name", file).
				Error("Unable to read file")
		} else if err == chronograf.ErrSourceInvalid {
			d.Logger.
				WithField("component", "source").
				WithField("name", file).
				Error("File is not a source")
		}
		return chronograf.Source{}, err
	}
	return board, nil
}

// Update replaces a source from the file system directory
func (d *Sources) Update(ctx context.Context, source chronograf.Source) error {
	board, _, err := d.idToFile(source.ID)
	if err != nil {
		return err
	}

	if err := d.Delete(ctx, board); err != nil {
		return err
	}
	file := sourceFile(d.Dir, source)
	return d.Create(file, source)
}

// idToFile takes an id and finds the associated filename
func (d *Sources) idToFile(id int) (chronograf.Source, string, error) {
	// Because the entire source information is not known at this point, we need
	// to try to find the name of the file through matching the ID in the source
	// content with the ID passed.
	files, err := d.ReadDir(d.Dir)
	if err != nil {
		return chronograf.Source{}, "", err
	}

	for _, f := range files {
		if path.Ext(f.Name()) != SrcExt {
			continue
		}
		file := path.Join(d.Dir, f.Name())
		var source chronograf.Source
		if err := d.Load(file, &source); err != nil {
			return chronograf.Source{}, "", err
		}
		if source.ID == id {
			return source, file, nil
		}
	}

	return chronograf.Source{}, "", chronograf.ErrSourceNotFound
}
