# papertrail webhooks

Enables Telegraf to act as a [Papertrail Webhook](http://help.papertrailapp.com/kb/how-it-works/web-hooks/).

## Events

[Full documentation](http://help.papertrailapp.com/kb/how-it-works/web-hooks/#callback).

Events from Papertrail come in two forms:

* The [event-based callback](http://help.papertrailapp.com/kb/how-it-works/web-hooks/#callback):

  * A point is created per event, with the timestamp as `received_at`
  * Each point has a field counter (`count`), which is set to `1` (signifying the event occurred)
  * Each event "hostname" object is converted to a `host` tag
  * The "saved_search" name in the payload is added as an `event` tag

* The [count-based callback](http://help.papertrailapp.com/kb/how-it-works/web-hooks/#count-only-webhooks)

  * A point is created per timeseries object per count, with the timestamp as the "timeseries" key (the unix epoch of the event)
  * Each point has a field counter (`count`), which is set to the value of each "timeseries" object
  * Each count "source_name" object is converted to a `host` tag
  * The "saved_search" name in the payload is added as an `event` tag

The current functionality is very basic, however this allows you to
track the number of events by host and saved search.

When an event is received, any point will look similar to:

```
papertrail,host=myserver.example.com,event=saved_search_name count=3i 1453248892000000000
```
