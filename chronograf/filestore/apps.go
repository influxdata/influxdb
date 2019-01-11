package filestore

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/influxdata/influxdb/chronograf"
)

// AppExt is the the file extension searched for in the directory for layout files
const AppExt = ".json"

// Apps are canned JSON layouts.  Implements LayoutsStore.
type Apps struct {
	Dir      string                                      // Dir is the directory contained the pre-canned applications.
	Load     func(string) (chronograf.Layout, error)     // Load loads string name and return a Layout
	Filename func(string, chronograf.Layout) string      // Filename takes dir and layout and returns loadable file
	Create   func(string, chronograf.Layout) error       // Create will write layout to file.
	ReadDir  func(dirname string) ([]os.FileInfo, error) // ReadDir reads the directory named by dirname and returns a list of directory entries sorted by filename.
	Remove   func(name string) error                     // Remove file
	IDs      chronograf.ID                               // IDs generate unique ids for new application layouts
	Logger   chronograf.Logger
}

// NewApps constructs a layout store wrapping a file system directory
func NewApps(dir string, ids chronograf.ID, logger chronograf.Logger) chronograf.LayoutsStore {
	return &Apps{
		Dir:      dir,
		Load:     loadFile,
		Filename: fileName,
		Create:   createLayout,
		ReadDir:  ioutil.ReadDir,
		Remove:   os.Remove,
		IDs:      ids,
		Logger:   logger,
	}
}

func fileName(dir string, layout chronograf.Layout) string {
	base := fmt.Sprintf("%s%s", layout.Measurement, AppExt)
	return path.Join(dir, base)
}

func loadFile(name string) (chronograf.Layout, error) {
	octets, err := ioutil.ReadFile(name)
	if err != nil {
		return chronograf.Layout{}, chronograf.ErrLayoutNotFound
	}
	var layout chronograf.Layout
	if err = json.Unmarshal(octets, &layout); err != nil {
		return chronograf.Layout{}, chronograf.ErrLayoutInvalid
	}
	return layout, nil
}

func createLayout(file string, layout chronograf.Layout) error {
	h, err := os.Create(file)
	if err != nil {
		return err
	}
	defer h.Close()
	if octets, err := json.MarshalIndent(layout, "    ", "    "); err != nil {
		return chronograf.ErrLayoutInvalid
	} else if _, err := h.Write(octets); err != nil {
		return err
	}

	return nil
}

// All returns all layouts from the directory
func (a *Apps) All(ctx context.Context) ([]chronograf.Layout, error) {
	files, err := a.ReadDir(a.Dir)
	if err != nil {
		return nil, err
	}

	layouts := []chronograf.Layout{}
	for _, file := range files {
		if path.Ext(file.Name()) != AppExt {
			continue
		}
		if layout, err := a.Load(path.Join(a.Dir, file.Name())); err != nil {
			continue // We want to load all files we can.
		} else {
			layouts = append(layouts, layout)
		}
	}
	return layouts, nil
}

// Add creates a new layout within the directory
func (a *Apps) Add(ctx context.Context, layout chronograf.Layout) (chronograf.Layout, error) {
	var err error
	layout.ID, err = a.IDs.Generate()
	if err != nil {
		a.Logger.
			WithField("component", "apps").
			Error("Unable to generate ID")
		return chronograf.Layout{}, err
	}
	file := a.Filename(a.Dir, layout)
	if err = a.Create(file, layout); err != nil {
		if err == chronograf.ErrLayoutInvalid {
			a.Logger.
				WithField("component", "apps").
				WithField("name", file).
				Error("Invalid Layout: ", err)
		} else {
			a.Logger.
				WithField("component", "apps").
				WithField("name", file).
				Error("Unable to write layout:", err)
		}
		return chronograf.Layout{}, err
	}
	return layout, nil
}

// Delete removes a layout file from the directory
func (a *Apps) Delete(ctx context.Context, layout chronograf.Layout) error {
	_, file, err := a.idToFile(layout.ID)
	if err != nil {
		return err
	}

	if err := a.Remove(file); err != nil {
		a.Logger.
			WithField("component", "apps").
			WithField("name", file).
			Error("Unable to remove layout:", err)
		return err
	}
	return nil
}

// Get returns an app file from the layout directory
func (a *Apps) Get(ctx context.Context, ID string) (chronograf.Layout, error) {
	l, file, err := a.idToFile(ID)
	if err != nil {
		return chronograf.Layout{}, err
	}

	if err != nil {
		if err == chronograf.ErrLayoutNotFound {
			a.Logger.
				WithField("component", "apps").
				WithField("name", file).
				Error("Unable to read file")
		} else if err == chronograf.ErrLayoutInvalid {
			a.Logger.
				WithField("component", "apps").
				WithField("name", file).
				Error("File is not a layout")
		}
		return chronograf.Layout{}, err
	}
	return l, nil
}

// Update replaces a layout from the file system directory
func (a *Apps) Update(ctx context.Context, layout chronograf.Layout) error {
	l, _, err := a.idToFile(layout.ID)
	if err != nil {
		return err
	}

	if err := a.Delete(ctx, l); err != nil {
		return err
	}
	file := a.Filename(a.Dir, layout)
	return a.Create(file, layout)
}

// idToFile takes an id and finds the associated filename
func (a *Apps) idToFile(ID string) (chronograf.Layout, string, error) {
	// Because the entire layout information is not known at this point, we need
	// to try to find the name of the file through matching the ID in the layout
	// content with the ID passed.
	files, err := a.ReadDir(a.Dir)
	if err != nil {
		return chronograf.Layout{}, "", err
	}

	for _, f := range files {
		if path.Ext(f.Name()) != AppExt {
			continue
		}
		file := path.Join(a.Dir, f.Name())
		layout, err := a.Load(file)
		if err != nil {
			return chronograf.Layout{}, "", err
		}
		if layout.ID == ID {
			return layout, file, nil
		}
	}

	return chronograf.Layout{}, "", chronograf.ErrLayoutNotFound
}
