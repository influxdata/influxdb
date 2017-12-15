package filestore

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"

	"github.com/influxdata/chronograf"
)

// DashExt is the the file extension searched for in the directory for dashboard files
const DashExt = ".dashboard"

var _ chronograf.DashboardsStore = &Dashboards{}

// Dashboards are JSON dashboards stored in the filesystem
type Dashboards struct {
	Dir      string                                      // Dir is the directory containing the dashboards.
	Load     func(string) (chronograf.Dashboard, error)  // Load loads string name and return a Dashboard
	Filename func(string, chronograf.Dashboard) string   // Filename takes dir and dashboard and returns loadable file
	Create   func(string, chronograf.Dashboard) error    // Create will write dashboard to file.
	ReadDir  func(dirname string) ([]os.FileInfo, error) // ReadDir reads the directory named by dirname and returns a list of directory entries sorted by filename.
	Remove   func(name string) error                     // Remove file
	IDs      chronograf.ID                               // IDs generate unique ids for new dashboards
	Logger   chronograf.Logger
}

// NewDashboards constructs a dashboard store wrapping a file system directory
func NewDashboards(dir string, ids chronograf.ID, logger chronograf.Logger) chronograf.DashboardsStore {
	return &Dashboards{
		Dir:      dir,
		Load:     dashboardLoad,
		Filename: dashboardFile,
		Create:   createDashboard,
		ReadDir:  ioutil.ReadDir,
		Remove:   os.Remove,
		IDs:      ids,
		Logger:   logger,
	}
}

func dashboardFile(dir string, dashboard chronograf.Dashboard) string {
	base := fmt.Sprintf("%s%s", dashboard.Name, DashExt)
	return path.Join(dir, base)
}

func dashboardLoad(name string) (chronograf.Dashboard, error) {
	octets, err := ioutil.ReadFile(name)
	if err != nil {
		return chronograf.Dashboard{}, chronograf.ErrDashboardNotFound
	}
	var dashboard chronograf.Dashboard
	if err = json.Unmarshal(octets, &dashboard); err != nil {
		return chronograf.Dashboard{}, chronograf.ErrDashboardInvalid
	}
	return dashboard, nil
}

func createDashboard(file string, dashboard chronograf.Dashboard) error {
	h, err := os.Create(file)
	if err != nil {
		return err
	}
	defer h.Close()
	if octets, err := json.MarshalIndent(dashboard, "    ", "    "); err != nil {
		return chronograf.ErrDashboardInvalid
	} else if _, err := h.Write(octets); err != nil {
		return err
	}

	return nil
}

// All returns all dashboards from the directory
func (d *Dashboards) All(ctx context.Context) ([]chronograf.Dashboard, error) {
	files, err := d.ReadDir(d.Dir)
	if err != nil {
		return nil, err
	}

	dashboards := []chronograf.Dashboard{}
	for _, file := range files {
		if path.Ext(file.Name()) != DashExt {
			continue
		}
		if dashboard, err := d.Load(path.Join(d.Dir, file.Name())); err != nil {
			continue // We want to load all files we can.
		} else {
			dashboards = append(dashboards, dashboard)
		}
	}
	return dashboards, nil
}

// Add creates a new dashboard within the directory
func (d *Dashboards) Add(ctx context.Context, dashboard chronograf.Dashboard) (chronograf.Dashboard, error) {
	genID, err := d.IDs.Generate()
	if err != nil {
		d.Logger.
			WithField("component", "dashboard").
			Error("Unable to generate ID")
		return chronograf.Dashboard{}, err
	}

	id, err := strconv.Atoi(genID)
	if err != nil {
		d.Logger.
			WithField("component", "dashboard").
			Error("Unable to convert ID")
		return chronograf.Dashboard{}, err
	}

	dashboard.ID = chronograf.DashboardID(id)

	file := d.Filename(d.Dir, dashboard)
	if err = d.Create(file, dashboard); err != nil {
		if err == chronograf.ErrDashboardInvalid {
			d.Logger.
				WithField("component", "dashboard").
				WithField("name", file).
				Error("Invalid Dashboard: ", err)
		} else {
			d.Logger.
				WithField("component", "dashboard").
				WithField("name", file).
				Error("Unable to write dashboard:", err)
		}
		return chronograf.Dashboard{}, err
	}
	return dashboard, nil
}

// Delete removes a dashboard file from the directory
func (d *Dashboards) Delete(ctx context.Context, dashboard chronograf.Dashboard) error {
	_, file, err := d.idToFile(dashboard.ID)
	if err != nil {
		return err
	}

	if err := d.Remove(file); err != nil {
		d.Logger.
			WithField("component", "dashboard").
			WithField("name", file).
			Error("Unable to remove dashboard:", err)
		return err
	}
	return nil
}

// Get returns a dashboard file from the dashboard directory
func (d *Dashboards) Get(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error) {
	board, file, err := d.idToFile(id)
	if err != nil {
		if err == chronograf.ErrDashboardNotFound {
			d.Logger.
				WithField("component", "dashboard").
				WithField("name", file).
				Error("Unable to read file")
		} else if err == chronograf.ErrDashboardInvalid {
			d.Logger.
				WithField("component", "dashboard").
				WithField("name", file).
				Error("File is not a dashboard")
		}
		return chronograf.Dashboard{}, err
	}
	return board, nil
}

// Update replaces a dashboard from the file system directory
func (d *Dashboards) Update(ctx context.Context, dashboard chronograf.Dashboard) error {
	board, _, err := d.idToFile(dashboard.ID)
	if err != nil {
		return err
	}

	if err := d.Delete(ctx, board); err != nil {
		return err
	}
	file := d.Filename(d.Dir, dashboard)
	return d.Create(file, dashboard)
}

// idToFile takes an id and finds the associated filename
func (d *Dashboards) idToFile(id chronograf.DashboardID) (chronograf.Dashboard, string, error) {
	// Because the entire dashboard information is not known at this point, we need
	// to try to find the name of the file through matching the ID in the dashboard
	// content with the ID passed.
	files, err := d.ReadDir(d.Dir)
	if err != nil {
		return chronograf.Dashboard{}, "", err
	}

	for _, f := range files {
		if path.Ext(f.Name()) != DashExt {
			continue
		}
		file := path.Join(d.Dir, f.Name())
		dashboard, err := d.Load(file)
		if err != nil {
			return chronograf.Dashboard{}, "", err
		}
		if dashboard.ID == id {
			return dashboard, file, nil
		}
	}

	return chronograf.Dashboard{}, "", chronograf.ErrDashboardNotFound
}
