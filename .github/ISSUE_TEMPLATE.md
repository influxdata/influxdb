### Directions
_GitHub Issues are reserved for actionable bug reports and feature requests._
_General questions should be sent to the [InfluxDB Community Site](https://community.influxdata.com)._

_Before opening an issue, search for similar bug reports or feature requests on GitHub Issues._
_If no similar issue can be found, fill out either the "Bug Report" or the "Feature Request" section below.
_Erase the other section and everything on and above this line._

### Bug report

__System info:__ [Include InfluxDB version, operating system name, and other relevant details]

__Steps to reproduce:__

1. [First Step]
2. [Second Step]
3. [and so on...]

__Expected behavior:__ [What you expected to happen]

__Actual behavior:__ [What actually happened]

__Additional info:__ [Include gist of relevant config, logs, etc.]

Also, if this is an issue of for performance, locking, etc the following commands are useful to create debug information for the team.

```
curl -o profiles.tar.gz "http://localhost:8086/debug/pprof/all?cpu=true"

curl -o vars.txt "http://localhost:8086/debug/vars"
iostat -xd 1 30 > iostat.txt
```

**Please note** It will take at least 30 seconds for the first cURL command above to return a response.
This is because it will run a CPU profile as part of its information gathering, which takes 30 seconds to collect.
Ideally you should run these commands when you're experiencing problems, so we can capture the state of the system at that time.

If you're concerned about running a CPU profile (which only has a small, temporary impact on performance), then you can set `?cpu=false` or omit `?cpu=true` altogether.

Please run those if possible and link them from a [gist](http://gist.github.com) or simply attach them as a comment to the issue.

*Please note, the quickest way to fix a bug is to open a Pull Request.*


### Feature Request

Opening a feature request kicks off a discussion.
Requests may be closed if we're not actively planning to work on them.

__Proposal:__ [Description of the feature]

__Current behavior:__ [What currently happens]

__Desired behavior:__ [What you would like to happen]

__Use case:__ [Why is this important (helps with prioritizing requests)]
