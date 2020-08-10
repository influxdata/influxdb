# Filestack webhook

You should configure your Filestack's Webhooks to point at the `webhooks` service. To do this go to [filestack.com](https://www.filestack.com/), select your app and click `Credentials > Webhooks`. In the resulting page, set the `URL` to `http://<my_ip>:1619/filestack`, and click on `Add`.

## Events

See the [webhook doc](https://www.filestack.com/docs/webhooks).

*Limitations*: It stores all events except video conversions events.

All events for logs the original timestamp, the action and the id.

**Tags:**
* 'action' = `event.action` string

**Fields:**
* 'id' = `event.id` string
