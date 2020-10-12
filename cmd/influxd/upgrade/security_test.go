package upgrade

import (
	"bufio"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestGenerateScript(t *testing.T) {

	type testCase struct {
		name    string
		users   []meta.UserInfo
		db2ids  map[string][]string
		skipExe bool
		want    string
		wantErr error
	}

	var testCases = []testCase{
		{
			name: "ordinary",
			users: []meta.UserInfo{
				{ // not upgraded because admin
					Name:  "superman",
					Admin: true,
				},
				{ // not upgraded because no privileges
					Name:  "loser",
					Admin: false,
				},
				{
					Name:  "weatherman",
					Admin: false,
					Privileges: map[string]influxql.Privilege{
						"water": influxql.AllPrivileges,
						"air":   influxql.AllPrivileges,
					},
				},
				{
					Name:  "hitgirl",
					Admin: false,
					Privileges: map[string]influxql.Privilege{
						"hits": influxql.WritePrivilege,
					},
				},
				{
					Name:  "boss@hits.org", // special name
					Admin: false,
					Privileges: map[string]influxql.Privilege{
						"hits": influxql.AllPrivileges,
					},
				},
				{
					Name:  "viewer",
					Admin: false,
					Privileges: map[string]influxql.Privilege{
						"water": influxql.ReadPrivilege,
						"air":   influxql.ReadPrivilege,
					},
				},
			},
			db2ids: map[string][]string{
				"water": {"33f9d67bc9cbc5b7", "33f9d67bc9cbc5b8", "33f9d67bc9cbc5b9"},
				"air":   {"43f9d67bc9cbc5b7", "43f9d67bc9cbc5b8", "43f9d67bc9cbc5b9"},
				"hits":  {"53f9d67bc9cbc5b7"},
			},
			want: testScriptShOrdinary,
		},
		{
			name: "missing buckets",
			users: []meta.UserInfo{
				{
					Name:  "weatherman",
					Admin: false,
					Privileges: map[string]influxql.Privilege{
						"water": influxql.AllPrivileges,
						"air":   influxql.AllPrivileges,
					},
				},
			},
			db2ids:  nil,
			wantErr: errors.New("upgrade: there were errors/warnings, please fix them and run the command again"),
		},
		{
			name:  "no users",
			users: []meta.UserInfo{},
			want:  "",
		},
		{
			name: "influx 2.x not found",
			users: []meta.UserInfo{
				{
					Name:  "dummy",
					Admin: false,
				},
			},
			skipExe: true,
			wantErr: errors.New("upgrade: version 2.x influx executable not found"),
		},
	}

	var shSuffix, mockInfluxCode string
	if runtime.GOOS == "win" {
		shSuffix = ".cmd"
		mockInfluxCode = testMockInfluxWin
	} else {
		shSuffix = ".sh"
		mockInfluxCode = testMockInfluxUnix
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) { // better do not run in parallel
			// mock v1 meta
			v1 := &influxDBv1{
				meta: &meta.Client{},
			}
			data := &meta.Data{
				Users: tc.users,
			}
			// inject users into mock v1 meta client
			f := reflect.ValueOf(v1.meta).Elem().Field(4)
			f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
			f.Set(reflect.ValueOf(data))

			// script output file
			tmpfile, err := ioutil.TempFile(t.TempDir(), "upgrade-security-*"+shSuffix)
			require.NoError(t, err)
			require.NoError(t, tmpfile.Close())

			// options passed on cmdline
			targetOptions := optionsV2{
				userName:           "admin",
				orgName:            "demo",
				token:              "ABC007==",
				securityScriptPath: tmpfile.Name(),
			}

			// create mock v2.x influx executable
			if !tc.skipExe {
				testExePath, err := os.Executable()
				require.NoError(t, err)
				mockInfluxExePath := filepath.Join(filepath.Dir(testExePath), "influx")
				file, err := os.OpenFile(mockInfluxExePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
				require.NoError(t, err)
				_, err = file.WriteString(mockInfluxCode)
				require.NoError(t, err)
				file.Close()
				defer os.Remove(mockInfluxExePath)
			}

			// command execution
			err = generateSecurityScript(v1, targetOptions, tc.db2ids, zaptest.NewLogger(t))
			if err != nil {
				if tc.wantErr != nil {
					if diff := cmp.Diff(tc.wantErr.Error(), err.Error()); diff != "" {
						t.Fatal(diff)
					}
				} else {
					t.Fatal(err)
				}
			} else if tc.wantErr != nil {
				t.Fatalf("should have with %v", tc.wantErr)
			}

			// validate result by comparing arrays of non-empty lines of wanted vs actual output
			parse := func(content string) []string {
				var lines []string
				scanner := bufio.NewScanner(strings.NewReader(content))
				for scanner.Scan() {
					line := strings.TrimSpace(scanner.Text())
					if line != "" {
						lines = append(lines, line)
					}
				}
				return lines
			}
			bs, err := ioutil.ReadFile(tmpfile.Name())
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(tc.want, "?") {
				// Generated security script contains path to `influx` executable
				// and this must be updated with test executable build path in the wanted result.
				exePath, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				tc.want = strings.Replace(tc.want, "?", filepath.Dir(exePath), -1)
			}
			expected := parse(tc.want)
			actual := parse(string(bs))
			if diff := cmp.Diff(expected, actual); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

var testScriptShOrdinary = `#!/bin/sh

UPGRADE_USER_boss_hits_org=yes # user boss@hits.org
UPGRADE_USER_hitgirl=yes # user hitgirl
UPGRADE_USER_viewer=yes # user viewer
UPGRADE_USER_weatherman=yes # user weatherman

# user superman is 1.x admin and will not be upgraded automatically
# user loser has no privileges and will be skipped

#
# SCRIPT VARS
#

INFLUX=?/influx
INFLUX_TOKEN=ABC007==
LOG="${0%.*}.$(date +%Y%m%d-%H%M%S).log"

#
# USERS UPGRADES
#

if [ "$UPGRADE_USER_boss_hits_org" = "yes" ]; then
    echo Creating authorization token for user boss@hits.org...
    env INFLUX_TOKEN=$INFLUX_TOKEN $INFLUX auth create --user=admin --org=demo --description="boss@hits.org" --read-bucket=53f9d67bc9cbc5b7 --write-bucket=53f9d67bc9cbc5b7
fi 2>&1 | tee -a $LOG

if [ "$UPGRADE_USER_hitgirl" = "yes" ]; then
    echo Creating authorization token for user hitgirl...
    env INFLUX_TOKEN=$INFLUX_TOKEN $INFLUX auth create --user=admin --org=demo --description="hitgirl" --write-bucket=53f9d67bc9cbc5b7
fi 2>&1 | tee -a $LOG

if [ "$UPGRADE_USER_viewer" = "yes" ]; then
    echo Creating authorization token for user viewer...
    env INFLUX_TOKEN=$INFLUX_TOKEN $INFLUX auth create --user=admin --org=demo --description="viewer" --read-bucket=43f9d67bc9cbc5b7 --read-bucket=43f9d67bc9cbc5b8 --read-bucket=43f9d67bc9cbc5b9 --read-bucket=33f9d67bc9cbc5b7 --read-bucket=33f9d67bc9cbc5b8 --read-bucket=33f9d67bc9cbc5b9
fi 2>&1 | tee -a $LOG

if [ "$UPGRADE_USER_weatherman" = "yes" ]; then
    echo Creating authorization token for user weatherman...
    env INFLUX_TOKEN=$INFLUX_TOKEN $INFLUX auth create --user=admin --org=demo --description="weatherman" --read-bucket=43f9d67bc9cbc5b7 --write-bucket=43f9d67bc9cbc5b7 --read-bucket=43f9d67bc9cbc5b8 --write-bucket=43f9d67bc9cbc5b8 --read-bucket=43f9d67bc9cbc5b9 --write-bucket=43f9d67bc9cbc5b9 --read-bucket=33f9d67bc9cbc5b7 --write-bucket=33f9d67bc9cbc5b7 --read-bucket=33f9d67bc9cbc5b8 --write-bucket=33f9d67bc9cbc5b8 --read-bucket=33f9d67bc9cbc5b9 --write-bucket=33f9d67bc9cbc5b9
fi 2>&1 | tee -a $LOG

echo
echo Output saved to $LOG
`

var testMockInfluxUnix = `#!/bin/sh
if [ "$1" = "version" ]; then
    echo Influx CLI 2.0.0
else
    exit 1
fi
`
var testMockInfluxWin = `@echo off
IF /I "%1" == "version" (
    echo "Influx CLI 2.0.0"
) else (
    exit 1
)
`
