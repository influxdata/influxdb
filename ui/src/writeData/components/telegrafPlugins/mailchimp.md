# Mailchimp Input

Pulls campaign reports from the [Mailchimp API](https://developer.mailchimp.com/).

### Configuration

This section contains the default TOML to configure the plugin.  You can
generate it using `telegraf --usage mailchimp`.

```toml
[[inputs.mailchimp]]
  ## MailChimp API key
  ## get from https://admin.mailchimp.com/account/api/
  api_key = "" # required
  
  ## Reports for campaigns sent more than days_old ago will not be collected.
  ## 0 means collect all and is the default value.
  days_old = 0
  
  ## Campaign ID to get, if empty gets all campaigns, this option overrides days_old
  # campaign_id = ""
```

### Metrics

- mailchimp
  - tags:
    - id
    - campaign_title
  - fields:
    - emails_sent (integer, emails)
    - abuse_reports (integer, reports)
    - unsubscribed (integer, unsubscribes)
    - hard_bounces (integer, emails)
    - soft_bounces (integer, emails)
    - syntax_errors (integer, errors)
    - forwards_count (integer, emails)
    - forwards_opens (integer, emails)
    - opens_total (integer, emails)
    - unique_opens (integer, emails)
    - open_rate (double, percentage)
    - clicks_total (integer, clicks)
    - unique_clicks (integer, clicks)
    - unique_subscriber_clicks (integer, clicks)
    - click_rate (double, percentage)
    - facebook_recipient_likes (integer, likes)
    - facebook_unique_likes (integer, likes)
    - facebook_likes (integer, likes)
    - industry_type (string, type)
    - industry_open_rate (double, percentage)
    - industry_click_rate (double, percentage)
    - industry_bounce_rate (double, percentage)
    - industry_unopen_rate (double, percentage)
    - industry_unsub_rate (double, percentage)
    - industry_abuse_rate (double, percentage)
    - list_stats_sub_rate (double, percentage)
    - list_stats_unsub_rate (double, percentage)
    - list_stats_open_rate (double, percentage)
    - list_stats_click_rate (double, percentage)
