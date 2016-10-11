package canned

//go:generate go-bindata -o apps_gen.go -ignore README|apps -pkg canned .

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"

	"github.com/influxdata/mrfusion"
	fusionlog "github.com/influxdata/mrfusion/log"
	"golang.org/x/net/context"
)

const AppExt = ".json"

var logger = fusionlog.New()

// Apps are canned JSON layouts.  Implements LayoutStore
type Apps struct {
	Dir      string                                      // Dir is the directory contained the pre-canned applications.
	Load     func(string) (mrfusion.Layout, error)       // Load loads string name and return a Layout
	Filename func(string, mrfusion.Layout) string        // Filename takes dir and layout and returns loadable file
	Create   func(string, mrfusion.Layout) error         // Create will write layout to file.
	ReadDir  func(dirname string) ([]os.FileInfo, error) // ReadDir reads the directory named by dirname and returns a list of directory entries sorted by filename.
	Remove   func(name string) error                     // Remove file
	IDs      mrfusion.ID                                 // IDs generate unique ids for new application layouts
}

func NewApps(dir string, ids mrfusion.ID) mrfusion.LayoutStore {
	return &Apps{
		Dir:      dir,
		Load:     loadFile,
		Filename: fileName,
		Create:   createLayout,
		ReadDir:  ioutil.ReadDir,
		Remove:   os.Remove,
		IDs:      ids,
	}
}

// NewBindataApps restores application layouts into dir and serves them there.
// If the file system is not permanent (e.g. Docker without a volume) changes will
// not persist.
func NewBindataApps(dir string, ids mrfusion.ID) mrfusion.LayoutStore {
	names := AssetNames()
	// Only restore the files that do not exist.
	// The idea is that any changes are preserved.
	for _, f := range names {
		// File doesn't exist so we try to restore.
		if _, err := os.Stat(f); err != nil {
			if err = RestoreAsset(dir, f); err != nil {
				// If app is not able to be restored, I want the keep trying for all the rest.
				logger.
					WithField("component", "apps").
					WithField("name", f).
					WithField("dir", dir).
					Info("Unable to restore app asset", err)
				continue
			}
		}
	}
	return NewApps(dir, ids)
}

func fileName(dir string, layout mrfusion.Layout) string {
	return path.Join(dir, layout.ID+AppExt)
}

func loadFile(name string) (mrfusion.Layout, error) {
	octets, err := ioutil.ReadFile(name)
	if err != nil {
		logger.
			WithField("component", "apps").
			WithField("name", name).
			Error("Unable to read file")
		return mrfusion.Layout{}, err
	}
	var layout mrfusion.Layout
	if err = json.Unmarshal(octets, &layout); err != nil {
		logger.
			WithField("component", "apps").
			WithField("name", name).
			Error("File is not a layout")
		return mrfusion.Layout{}, err
	}
	return layout, nil
}

func createLayout(file string, layout mrfusion.Layout) error {
	h, err := os.Create(file)
	if err != nil {
		return err
	}
	defer h.Close()
	if octets, err := json.MarshalIndent(layout, "    ", "    "); err != nil {
		logger.
			WithField("component", "apps").
			WithField("name", file).
			Error("Unable to marshal layout:", err)
		return err
	} else {
		if _, err := h.Write(octets); err != nil {
			logger.
				WithField("component", "apps").
				WithField("name", file).
				Error("Unable to write layout:", err)
			return err
		}
	}
	return nil
}

func (a *Apps) All(ctx context.Context) ([]mrfusion.Layout, error) {
	files, err := a.ReadDir(a.Dir)
	if err != nil {
		return nil, err
	}

	layouts := []mrfusion.Layout{}
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

func (a *Apps) Add(ctx context.Context, layout mrfusion.Layout) (mrfusion.Layout, error) {
	var err error
	layout.ID, err = a.IDs.Generate()
	file := a.Filename(a.Dir, layout)
	if err = a.Create(file, layout); err != nil {
		return mrfusion.Layout{}, err
	}
	return layout, nil
}

func (a *Apps) Delete(ctx context.Context, layout mrfusion.Layout) error {
	file := a.Filename(a.Dir, layout)
	if err := a.Remove(file); err != nil {
		logger.
			WithField("component", "apps").
			WithField("name", file).
			Error("Unable to remove layout:", err)
		return err
	}
	return nil
}

func (a *Apps) Get(ctx context.Context, ID string) (mrfusion.Layout, error) {
	file := a.Filename(a.Dir, mrfusion.Layout{ID: ID})
	l, err := a.Load(file)
	if err != nil {
		return mrfusion.Layout{}, mrfusion.ErrLayoutNotFound
	}
	return l, nil
}

func (a *Apps) Update(ctx context.Context, layout mrfusion.Layout) error {
	if err := a.Delete(ctx, layout); err != nil {
		return err
	}
	file := a.Filename(a.Dir, layout)
	return a.Create(file, layout)
}
