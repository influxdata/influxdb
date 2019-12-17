package plugins

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAvailablePlugins(t *testing.T) {
	cases, err := AvailablePlugins()
	require.NoError(t, err)
	require.Equal(t, 223, len(cases.Plugins))
}

func TestAvailableBundles(t *testing.T) {
	cases, err := AvailableBundles()
	require.NoError(t, err)
	require.Equal(t, 1, len(cases.Plugins))
}

func TestGetPlugin(t *testing.T) {
	tests := []struct {
		typ      string
		name     string
		expected string
		ok       bool
	}{
		{
			typ:      "input",
			name:     "cpu",
			expected: "# Read metrics about cpu usage\n[[inputs.cpu]]\n  # alias=\"cpu\"\n  ## Whether to report per-cpu stats or not\n  percpu = true\n  ## Whether to report total system cpu stats or not\n  totalcpu = true\n  ## If true, collect raw CPU time metrics.\n  collect_cpu_time = false\n  ## If true, compute and report the sum of all non-idle CPU states.\n  report_active = false\n\n",
			ok:       true,
		},
		{
			typ:      "output",
			name:     "file",
			expected: "# Send telegraf metrics to file(s)\n[[outputs.file]]\n  # alias=\"file\"\n  ## Files to write to, \"stdout\" is a specially handled file.\n  files = [\"stdout\", \"/tmp/metrics.out\"]\n\n  ## Use batch serialization format instead of line based delimiting.  The\n  ## batch format allows for the production of non line based output formats and\n  ## may more effiently encode metric groups.\n  # use_batch_format = false\n\n  ## The file will be rotated after the time interval specified.  When set\n  ## to 0 no time based rotation is performed.\n  # rotation_interval = \"0d\"\n\n  ## The logfile will be rotated when it becomes larger than the specified\n  ## size.  When set to 0 no size based rotation is performed.\n  # rotation_max_size = \"0MB\"\n\n  ## Maximum number of rotated archives to keep, any older logs are deleted.\n  ## If set to -1, no archives are removed.\n  # rotation_max_archives = 5\n\n  ## Data format to output.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  data_format = \"influx\"\n\n",
			ok:       true,
		},
		{
			typ:      "processor",
			name:     "converter",
			expected: "# Convert values to another metric value type\n[[processors.converter]]\n  # alias=\"converter\"\n  ## Tags to convert\n  ##\n  ## The table key determines the target type, and the array of key-values\n  ## select the keys to convert.  The array may contain globs.\n  ##   <target-type> = [<tag-key>...]\n  [processors.converter.tags]\n    string = []\n    integer = []\n    unsigned = []\n    boolean = []\n    float = []\n\n  ## Fields to convert\n  ##\n  ## The table key determines the target type, and the array of key-values\n  ## select the keys to convert.  The array may contain globs.\n  ##   <target-type> = [<field-key>...]\n  [processors.converter.fields]\n    tag = []\n    string = []\n    integer = []\n    unsigned = []\n    boolean = []\n    float = []\n\n",
			ok:       true,
		},
		{
			typ:      "aggregator",
			name:     "merge",
			expected: "# Merge metrics into multifield metrics by series key\n[[aggregators.merge]]\n  # alias=\"merge\"\n",
			ok:       true,
		},
		{
			typ:      "bundle",
			name:     "System Bundle",
			expected: "# Read metrics about cpu usage\n[[inputs.cpu]]\n  # alias=\"cpu\"\n  ## Whether to report per-cpu stats or not\n  percpu = true\n  ## Whether to report total system cpu stats or not\n  totalcpu = true\n  ## If true, collect raw CPU time metrics.\n  collect_cpu_time = false\n  ## If true, compute and report the sum of all non-idle CPU states.\n  report_active = false\n# Read metrics about swap memory usage\n[[inputs.swap]]\n  # alias=\"swap\"\n# Read metrics about disk usage by mount point\n[[inputs.disk]]\n  # alias=\"disk\"\n  ## By default stats will be gathered for all mount points.\n  ## Set mount_points will restrict the stats to only the specified mount points.\n  # mount_points = [\"/\"]\n\n  ## Ignore mount points by filesystem type.\n  ignore_fs = [\"tmpfs\", \"devtmpfs\", \"devfs\", \"iso9660\", \"overlay\", \"aufs\", \"squashfs\"]\n# Read metrics about memory usage\n[[inputs.mem]]\n  # alias=\"mem\"\n",
			ok:       true,
		},
		{
			typ:      "input",
			name:     "not-a-real-plugin",
			expected: "",
			ok:       false,
		},
	}
	for _, test := range tests {
		p, ok := GetPlugin(test.typ, test.name)
		require.Equal(t, test.ok, ok)
		if ok {
			require.Equal(t, test.expected, p.Config)
		}
	}
}

func TestListAvailablePlugins(t *testing.T) {
	tests := []struct {
		typ      string
		expected int
		err      string
	}{
		{
			typ:      "input",
			expected: 170,
		},
		{
			typ:      "output",
			expected: 33,
		},
		{
			typ:      "processor",
			expected: 14,
		},
		{
			typ:      "aggregator",
			expected: 6,
		},
		{
			typ:      "bundle",
			expected: 1,
		},
		{
			typ:      "other",
			expected: 0,
			err:      "unknown plugin type 'other'",
		},
	}
	for _, test := range tests {
		p, err := ListAvailablePlugins(test.typ)
		if err != nil {
			require.Equal(t, test.err, err.Error())
		} else {
			require.Equal(t, test.expected, len(p.Plugins))
		}
	}
}
