# Logparser Input Plugin

The `logparser` plugin streams and parses the given logfiles. Currently it
has the capability of parsing "grok" patterns from logfiles, which also supports
regex patterns.

**Deprecated in Telegraf 1.15**: Please use the [tail][] plugin along with the [`grok` data format][grok parser].

The `tail` plugin now provides all the functionality of the `logparser` plugin.
Most options can be translated directly to the `tail` plugin:
- For options in the `[inputs.logparser.grok]` section, the equivalent option
  will have add the `grok_` prefix when using them in the `tail` input.
- The grok `measurement` option can be replaced using the standard plugin
  `name_override` option.

Migration Example:
```diff
- [[inputs.logparser]]
-   files = ["/var/log/apache/access.log"]
-   from_beginning = false
-   [inputs.logparser.grok]
-     patterns = ["%{COMBINED_LOG_FORMAT}"]
-     measurement = "apache_access_log"
-     custom_pattern_files = []
-     custom_patterns = '''
-     '''
-     timezone = "Canada/Eastern"

+ [[inputs.tail]]
+   files = ["/var/log/apache/access.log"]
+   from_beginning = false
+   grok_patterns = ["%{COMBINED_LOG_FORMAT}"]
+   name_override = "apache_access_log"
+   grok_custom_pattern_files = []
+   grok_custom_patterns = '''
+   '''
+   grok_timezone = "Canada/Eastern"
+   data_format = "grok"
```

### Configuration

```toml
[[inputs.logparser]]
  ## Log files to parse.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**.log     -> recursively find all .log files in /var/log
  ##   /var/log/*/*.log    -> find all .log files with a parent dir in /var/log
  ##   /var/log/apache.log -> only tail the apache log file
  files = ["/var/log/apache/access.log"]

  ## Read files that currently exist from the beginning. Files that are created
  ## while telegraf is running (and that match the "files" globs) will always
  ## be read from the beginning.
  from_beginning = false

  ## Method used to watch for file updates.  Can be either "inotify" or "poll".
  # watch_method = "inotify"

  ## Parse logstash-style "grok" patterns:
  [inputs.logparser.grok]
    ## This is a list of patterns to check the given log file(s) for.
    ## Note that adding patterns here increases processing time. The most
    ## efficient configuration is to have one pattern per logparser.
    ## Other common built-in patterns are:
    ##   %{COMMON_LOG_FORMAT}   (plain apache & nginx access logs)
    ##   %{COMBINED_LOG_FORMAT} (access logs + referrer & agent)
    patterns = ["%{COMBINED_LOG_FORMAT}"]

    ## Name of the outputted measurement name.
    measurement = "apache_access_log"

    ## Full path(s) to custom pattern files.
    custom_pattern_files = []

    ## Custom patterns can also be defined here. Put one pattern per line.
    custom_patterns = '''
    '''

    ## Timezone allows you to provide an override for timestamps that
    ## don't already include an offset
    ## e.g. 04/06/2016 12:41:45 data one two 5.43µs
    ##
    ## Default: "" which renders UTC
    ## Options are as follows:
    ##   1. Local             -- interpret based on machine localtime
    ##   2. "Canada/Eastern"  -- Unix TZ values like those found in https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    ##   3. UTC               -- or blank/unspecified, will return timestamp in UTC
    # timezone = "Canada/Eastern"
```

### Grok Parser

Reference the [grok parser][] documentation to setup the grok section of the
configuration.


### Additional Resources

- https://www.influxdata.com/telegraf-correlate-log-metrics-data-performance-bottlenecks/

[tail]: /plugins/inputs/tail/README.md
[grok parser]: /plugins/parsers/grok/README.md
