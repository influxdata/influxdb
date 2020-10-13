package upgrade

// Security upgrade implementation.
// Generates script for upgrading 1.x users into tokens in 2.x.

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"text/template"

	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// generateSecurityScript creates security upgrade script with 1.x users mapped to 2.x tokens.
func generateSecurityScript(v1 *influxDBv1, targetOptions optionsV2, dbBuckets map[string][]string, log *zap.Logger) error {
	// check if there any 1.x users at all
	v1meta := v1.meta
	if len(v1meta.Users()) == 0 {
		log.Info("There are no users in 1.x, no script will be created.")
		return nil
	}

	// get helper instance
	helper := newSecurityScriptHelper(log)

	// check if target buckets exists in 2.x
	proceed := helper.checkDbBuckets(v1meta, dbBuckets)
	if !proceed {
		return errors.New("upgrade: there were errors/warnings, please fix them and run the command again")
	}

	// create output
	var output *os.File
	var isFileOutput bool
	if targetOptions.securityScriptPath == "" {
		output = os.Stdout
	} else {
		file, err := os.OpenFile(targetOptions.securityScriptPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
		if err != nil {
			return err
		}
		defer file.Close()
		output = file
		isFileOutput = true
	}

	// template data type
	type dataObject struct {
		AdminUsers   []string
		IgnoreUsers  []string
		UpgradeUsers []string
		UserArgs     map[string][]string
		Exe          string
		TargetAdmin  string
		TargetOrg    string
		TargetToken  string
		IsFileOutput bool
		InfluxExe    string
	}

	// data
	data := dataObject{
		UserArgs:     make(map[string][]string),
		TargetAdmin:  targetOptions.userName,
		TargetOrg:    targetOptions.orgName,
		TargetToken:  targetOptions.token,
		IsFileOutput: isFileOutput,
		InfluxExe:    targetOptions.influx2CommandPath,
	}

	// fill data with users and their permissions
	for _, row := range helper.sortUserInfo(v1meta.Users()) {
		username := row.Name
		if row.Admin {
			data.AdminUsers = append(data.AdminUsers, username)
		} else if len(row.Privileges) == 0 {
			data.IgnoreUsers = append(data.IgnoreUsers, username)
		} else {
			data.UpgradeUsers = append(data.UpgradeUsers, username)
			dbList := make([]string, 0)
			for database := range row.Privileges {
				dbList = append(dbList, database)
			}
			sort.Strings(dbList)
			accessArgs := make([]string, 0)
			for _, database := range dbList {
				permission := row.Privileges[database]
				for _, id := range dbBuckets[database] {
					switch permission {
					case influxql.ReadPrivilege:
						accessArgs = append(accessArgs, fmt.Sprintf("--read-bucket=%s", id))
					case influxql.WritePrivilege:
						accessArgs = append(accessArgs, fmt.Sprintf("--write-bucket=%s", id))
					case influxql.AllPrivileges:
						accessArgs = append(accessArgs, fmt.Sprintf("--read-bucket=%s", id))
						accessArgs = append(accessArgs, fmt.Sprintf("--write-bucket=%s", id))
					}
				}
			}
			if len(accessArgs) > 0 { // should always be true
				data.UserArgs[username] = accessArgs
			}
		}
	}

	// load template
	var tmpl string
	if runtime.GOOS == "win" {
		tmpl = securityScriptCmdTemplate
	} else {
		tmpl = securityScriptShTemplate
	}
	t, err := template.New("script").Funcs(template.FuncMap{
		"shUserVar": func(name string) string {
			return helper.shUserVar(name)
		},
		"userArgs": func(args []string) string {
			return strings.Join(args, " ")
		},
	}).Parse(tmpl)
	if err != nil {
		return err
	}

	// generate the script
	err = t.Execute(output, data)
	if err != nil {
		return err
	}

	if isFileOutput {
		log.Info(fmt.Sprintf("Security upgrade script saved to %s.", targetOptions.securityScriptPath))
	}

	return nil
}

// securityScriptHelper is a helper used by `generate-security-script` command.
type securityScriptHelper struct {
	shReg *regexp.Regexp
	log   *zap.Logger
}

// newSecurityScriptHelper returns new security script helper instance for `generate-security-script` command.
func newSecurityScriptHelper(log *zap.Logger) *securityScriptHelper {
	helper := &securityScriptHelper{
		log: log,
	}
	helper.shReg = regexp.MustCompile("[^a-zA-Z0-9]+")

	return helper
}
func (h *securityScriptHelper) checkDbBuckets(meta *meta.Client, databases map[string][]string) bool {
	ok := true
	for _, row := range meta.Users() {
		for database := range row.Privileges {
			if database == "_internal" {
				continue
			}
			ids := databases[database]
			if len(ids) == 0 {
				h.log.Warn(fmt.Sprintf("No buckets for database [%s] exist in 2.x.", database))
				ok = false
			}
		}
	}

	return ok
}

func (h *securityScriptHelper) sortUserInfo(info []meta.UserInfo) []meta.UserInfo {
	sort.Slice(info, func(i, j int) bool {
		return info[i].Name < info[j].Name
	})
	return info
}

func (h *securityScriptHelper) shUserVar(name string) string {
	return "UPGRADE_USER_" + h.shReg.ReplaceAllString(name, "_")
}

// script templates

var securityScriptShTemplate = `#!/bin/sh

{{- range $u := $.UpgradeUsers}}
{{shUserVar $u}}=yes # user {{$u}}
{{- end}}

{{- range $u := $.AdminUsers}}
# user {{$u}} is 1.x admin and will not be upgraded automatically
{{- end}}
{{- range $u := $.IgnoreUsers}}
# user {{$u}} has no privileges and will be skipped
{{- end}}

#
# SCRIPT VARS
#

INFLUX={{$.InfluxExe}}
INFLUX_TOKEN={{$.TargetToken}}

{{- if $.IsFileOutput}}
LOG="${0%.*}.$(date +%Y%m%d-%H%M%S).log"
{{end}}

#
# USERS UPGRADES
#

{{range $u := $.UpgradeUsers}}
if [ "${{shUserVar $u}}" = "yes" ]; then
    echo Creating authorization token for user {{$u}}...
    env INFLUX_TOKEN=$INFLUX_TOKEN $INFLUX auth create --user={{$.TargetAdmin}} --org={{$.TargetOrg}} --description="{{$u}}" {{index $.UserArgs $u | userArgs}}
fi {{- if $.IsFileOutput}} 2>&1 | tee -a $LOG{{- end}}

{{- end}}

{{- if $.IsFileOutput}}
echo
echo Output saved to $LOG
{{- end}}
`

var securityScriptCmdTemplate = `@ECHO OFF

{{- range $u := $.UpgradeUsers}}
REM user {{$u}}
set {{shUserVar $u}}=yes
{{- end}}

{{- range $u := $.AdminUsers}}
REM user {{$u}} is 1.x admin and will not be upgraded automatically
{{- end}}
{{- range $u := $.IgnoreUsers}}
REM user {{$u}} has no privileges will be skipped
{{- end}}

REM
REM SCRIPT VARS
REM

set INFLUX="{{$.InfluxExe}}.exe"
set INFLUX_TOKEN={{$.TargetToken}}

{{- if $.IsFileOutput}}
set PATH=%PATH%;C:\WINDOWS\system32\wbem
for /f %%x in ('wmic os get localdatetime ^| findstr /b [0-9]') do @set X=%%x && set LOG=%~dpn0.%X:~0,8%-%X:~8,6%.log
{{end}}

REM
REM INDIVIDUAL USERS UPGRADES
REM

{{range $u := $.UpgradeUsers}}
IF /I "%{{shUserVar $u}}%" == "yes" (
    echo Creating authorization token for user {{$u}}...
    %INFLUX% auth create --user={{$.TargetAdmin}} --org={{$.TargetOrg}} --description="{{$u}}" {{index $.UserArgs $u | userArgs}}
) {{- if $.IsFileOutput}} >> %LOG% 2>&1 {{- end}}

{{- end}}

{{- if $.IsFileOutput}}
type %LOG%
echo.
echo Output saved to %LOG%
{{- end}}
`
