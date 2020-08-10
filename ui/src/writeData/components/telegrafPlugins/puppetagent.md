## Telegraf Plugin: PuppetAgent

#### Description

The puppetagent plugin collects variables outputted from the 'last_run_summary.yaml' file
usually located in `/var/lib/puppet/state/`
[PuppetAgent Runs](https://puppetlabs.com/blog/puppet-monitoring-how-to-monitor-the-success-or-failure-of-puppet-runs).

```
cat /var/lib/puppet/state/last_run_summary.yaml

---
  events:
    failure: 0
    total: 0
    success: 0
  resources:
    failed: 0
    scheduled: 0
    changed: 0
    skipped: 0
    total: 109
    failed_to_restart: 0
    restarted: 0
    out_of_sync: 0
  changes:
    total: 0
  time:
    user: 0.004331
    schedule: 0.001123
    filebucket: 0.000353
    file: 0.441472
    exec: 0.508123
    anchor: 0.000555
    yumrepo: 0.006989
    ssh_authorized_key: 0.000764
    service: 1.807795
    package: 1.325788
    total: 8.85354707064819
    config_retrieval: 4.75567007064819
    last_run: 1444936531
    cron: 0.000584
  version:
    config: 1444936521
    puppet: "3.7.5"
```

```
jcross@pit-devops-02 ~ >sudo ./telegraf_linux_amd64 --input-filter puppetagent --config tele.conf --test
* Plugin: puppetagent, Collection 1
> [] puppetagent_events_failure value=0
> [] puppetagent_events_total value=0
> [] puppetagent_events_success value=0
> [] puppetagent_resources_failed value=0
> [] puppetagent_resources_scheduled value=0
> [] puppetagent_resources_changed value=0
> [] puppetagent_resources_skipped value=0
> [] puppetagent_resources_total value=109
> [] puppetagent_resources_failedtorestart value=0
> [] puppetagent_resources_restarted value=0
> [] puppetagent_resources_outofsync value=0
> [] puppetagent_changes_total value=0
> [] puppetagent_time_user value=0.00393
> [] puppetagent_time_schedule value=0.001234
> [] puppetagent_time_filebucket value=0.000244
> [] puppetagent_time_file value=0.587734
> [] puppetagent_time_exec value=0.389584
> [] puppetagent_time_anchor value=0.000399
> [] puppetagent_time_sshauthorizedkey value=0.000655
> [] puppetagent_time_service value=0
> [] puppetagent_time_package value=1.297537
> [] puppetagent_time_total value=9.45297606225586
> [] puppetagent_time_configretrieval value=5.89822006225586
> [] puppetagent_time_lastrun value=1444940131
> [] puppetagent_time_cron value=0.000646
> [] puppetagent_version_config value=1444940121
> [] puppetagent_version_puppet value=3.7.5
```

## Measurements:
#### PuppetAgent int64 measurements:

Meta:
- units: int64
- tags: ``

Measurement names:
 - puppetagent_events_failure
 - puppetagent_events_total
 - puppetagent_events_success
 - puppetagent_resources_failed
 - puppetagent_resources_scheduled
 - puppetagent_resources_changed
 - puppetagent_resources_skipped
 - puppetagent_resources_total
 - puppetagent_resources_failedtorestart
 - puppetagent_resources_restarted
 - puppetagent_resources_outofsync
 - puppetagent_changes_total
 - puppetagent_time_service
 - puppetagent_time_lastrun
 - puppetagent_version_config

#### PuppetAgent float64 measurements:

Meta:
- units: float64
- tags: ``

Measurement names:
 - puppetagent_time_user
 - puppetagent_time_schedule
 - puppetagent_time_filebucket
 - puppetagent_time_file
 - puppetagent_time_exec
 - puppetagent_time_anchor
 - puppetagent_time_sshauthorizedkey
 - puppetagent_time_package
 - puppetagent_time_total
 - puppetagent_time_configretrieval
 - puppetagent_time_lastrun
 - puppetagent_time_cron
 - puppetagent_version_config

#### PuppetAgent string measurements:

Meta:
- units: string
- tags: ``

Measurement names:
 - puppetagent_version_puppet
