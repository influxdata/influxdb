# Tail Input Plugin

The tail plugin "tails" a logfile and parses each log message.

By default, the tail plugin acts like the following unix tail command:

```
tail -F --lines=0 myfile.log
```

- `-F` means that it will follow the _name_ of the given file, so
that it will be compatible with log-rotated files, and that it will retry on
inaccessible files.
- `--lines=0` means that it will start at the end of the file (unless
the `from_beginning` option is set).

see http://man7.org/linux/man-pages/man1/tail.1.html for more details.

The plugin expects messages in one of the
[Telegraf Input Data Formats](https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md).

### Configuration

```toml
[[inputs.tail]]
  ## File names or a pattern to tail.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   "/var/log/**.log"  -> recursively find all .log files in /var/log
  ##   "/var/log/*/*.log" -> find all .log files with a parent dir in /var/log
  ##   "/var/log/apache.log" -> just tail the apache log file
  ##
  ## See https://github.com/gobwas/glob for more examples
  ##
  files = ["/var/mymetrics.out"]

  ## Read file from beginning.
  # from_beginning = false

  ## Whether file is a named pipe
  # pipe = false

  ## Method used to watch for file updates.  Can be either "inotify" or "poll".
  # watch_method = "inotify"

  ## Maximum lines of the file to process that have not yet be written by the
  ## output.  For best throughput set based on the number of metrics on each
  ## line and the size of the output's metric_batch_size.
  # max_undelivered_lines = 1000

  ## Character encoding to use when interpreting the file contents.  Invalid
  ## characters are replaced using the unicode replacement character.  When set
  ## to the empty string the data is not decoded to text.
  ##   ex: character_encoding = "utf-8"
  ##       character_encoding = "utf-16le"
  ##       character_encoding = "utf-16be"
  ##       character_encoding = ""
  # character_encoding = ""

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"

  ## multiline parser/codec
  ## https://www.elastic.co/guide/en/logstash/2.4/plugins-filters-multiline.html
  #[inputs.tail.multiline]
    ## The pattern should be a regexp which matches what you believe to be an indicator that the field is part of an event consisting of multiple lines of log data.
    #pattern = "^\s"

    ## The field's value must be previous or next and indicates the relation to the
    ## multi-line event.
    #match_which_line = "previous"

    ## The invert_match can be true or false (defaults to false). 
    ## If true, a message not matching the pattern will constitute a match of the multiline filter and the what will be applied. (vice-versa is also true)
    #invert_match = false

    #After the specified timeout, this plugin sends the multiline event even if no new pattern is found to start a new event. The default is 5s.
    #timeout = 5s
```

### Metrics

Metrics are produced according to the `data_format` option.  Additionally a
tag labeled `path` is added to the metric containing the filename being tailed.
