// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import Editor from 'src/shared/components/TomlMonacoEditor'
import BucketDropdown from 'src/dataLoaders/components/BucketsDropdown'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {AppState, Bucket} from 'src/types'
import {
  Input,
  IconFont,
  Grid,
  Columns,
  Tabs,
  ComponentSize,
} from '@influxdata/clockface'

import './TelegrafEditor.scss'

type PluginType = 'system' | 'bundle' | 'input' | 'output' | 'processor' | 'aggregator' | 'display'
type Plugin = {
  name: string
  description: string
  code: string
  type: PluginType
}
type BundlePlugin = {
  name: string
  description: string
  type: 'bundle'
  includes: Plugin[]
}
type ValidPlugin = Plugin | BundlePlugin

interface StateProps {
  buckets: Bucket[]
  search: string
  script: string
  plugins: ValidPlugin[]
}

type Props = StateProps

const PLUGIN_REGEX = /\[\[\s*(inputs|outputs|processors|aggregators)\.(.+)\s*\]\]/

@ErrorHandling
class TelegrafEditor extends PureComponent<Props> {
  state = {
    isAdding: true
  }

  render() {
    const selected = null
    const { search, script } = this.props

    return (
      <Grid style={{ height: '100%' }}>
        <Grid.Row style={{ textAlign: 'center' }}>
          <h3 className="wizard-step--title">What do you want to monitor?</h3>
          <h5 className="wizard-step--sub-title">
            Telegraf is a plugin-based data collection agent which writes
            metrics to a bucket in InfluxDB
            <br />
            Use the editor below to configure as many of the 200+
            {' '}<a
              href="https://v2.docs.influxdata.com/v2.0/reference/telegraf-plugins/#input-plugins"
              target="_blank"
            >
              plugins
            </a>{' '}as you require
          </h5>
        </Grid.Row>
        <Grid.Row style={{ height: 'calc(100% - 128px)' }}>
          <Grid.Column widthXS={Columns.Three} style={{height:'100%'}}>
            <BucketDropdown
              selectedBucketID={selected}
              buckets={buckets}
              onSelectBucket={this.handleSelectBucket}
            />
            <Input
              className="wizard-step--filter"
              size={ComponentSize.Small}
              icon={IconFont.Search}
              value={search}
              onBlur={this.handleFilterBlur}
              onChange={this.handleFilterChange}
              placeholder="Filter Plugins..."
            />
            <Tabs.Container style={{height: 'calc(100% - 96px)', marginTop: '18px' }}>
              <Tabs>
                <Tabs.Tab text="Plugin Lookup" active={ !this.state.isAdding } onClick={ this.setAdding.bind(this, false) }/>
                <Tabs.Tab text="Add Plugins" active={ this.state.isAdding } onClick={ this.setAdding.bind(this, true) }/>
              </Tabs>
              <Tabs.TabContents>
                <FancyScrollbar
                  autoHide={false}
                  className="telegraf-editor--plugins"
                >
                  { !this.state.isAdding &&
                    this.renderPluginLocation() }
                  { this.state.isAdding &&
                    this.renderPluginList() }
                </FancyScrollbar>
              </Tabs.TabContents>
            </Tabs.Container>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Nine} style={{height: '100%'}}>
            <Editor script={script}
              onChangeScript={ this.extractPluginList.bind(this) }
              willMount={ this.connect.bind(this) }/>
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private setAdding(state) {
    this.setState({
      isAdding: state
    })
  }

  private renderPluginList() {
    const map = this.props.plugins.reduce((prev, curr) => {
      if (curr.name === '__default__') {
        return prev
      }

      if (!prev.hasOwnProperty(curr.type)) {
        prev[curr.type] = []
      }
      prev[curr.type].push(curr)

      return prev
    }, {})

    return [
      'bundle',
      'input',
      'output',
      'processor',
      'aggregator'
    ].map(k => {
      return {
        category: k,
        items: map[k] || []
      }
    })
    .filter(k => k.items.length)
    .reduce((prev, curr) => {
      prev.push({
        type: 'display',
        name: '-- ' + curr.category + ' --'
      })

      const items = curr.items.slice(0).sort((a, b) => {
        return a.name.localeCompare(b.name)
      })
      .filter(a => true)

      prev.push.apply(prev, items)

      return prev
    }, [])
    .map((k) => {
      if (k.type === 'display') {
        return (
          <div className={ k.type }
            key={ `_plugin_${k.type}.${k.name}` }>
            { k.name }
          </div>
        )
      }

      return (
        <div className={ k.type }
          key={ `_plugin_${k.type}.${k.name}` }
          onClick={ () => this.handleAdd(k) }
        >
          {k.name}
          <label>{k.description}</label>
        </div>
      )
    })
  }

  private renderPluginLocation() {
    return <h1>connecto to redux first</h1>
  }

  private extractPluginList() {
    if (!this._editor) {
      return;
    }
    const matches = this._editor.getModel().findMatches(
      PLUGIN_REGEX,
      false,
      true,
      false,
      null,
      true
    )

    const plugins = matches.map(m => {
      return {
        type: m.matches[1],
        name: m.matches[2],
        line: m.range.startLineNumber
      }
    })

    console.log('matchy matchy', plugins)
  }

  private connect(editor) {
    this._editor = editor
    this.extractPluginList()
  }

  private handleAdd(which) {
    const position = this._editor.getPosition()
    const matches = this._editor.getModel().findNextMatch(
      PLUGIN_REGEX,
      position,
      true,
      false,
      null,
      true
    )
    let lineNumber

    if (position.lineNumber === 1 || !matches || position.lineNumber > matches.range.startLineNumber) {
      //add it to the bottom
      lineNumber = this._editor.getModel().getLineCount()
    } else {
      lineNumber = matches.range.startLineNumber - 1
    }

    this._editor.setPosition({column: 1, lineNumber});

    if (which.type === 'bundle') {
      const map = this.props.plugins.reduce((prev, curr) => {
        prev[curr.name] = curr;
        return prev
      }, {})

      which.include
        .map(item => map[item].code)
        .reverse()
        .forEach(item => {
          this._editor.executeEdits('', [{
            range: {
              startLineNumber: lineNumber,
              startColumn: 1,
              endLineNumber: lineNumber,
              endColumn: 1
            },
            text: item,
            forceMoveMarkers: true
          }]);
        })
    } else {
      this._editor.executeEdits('', [{
        range: {
          startLineNumber: lineNumber,
          startColumn: 1,
          endLineNumber: lineNumber,
          endColumn: 1
        },
        text: which.code,
        forceMoveMarkers: true
      }]);
    }

    this._editor.revealLineInCenter(lineNumber);
  }

  private handleJump(which) {
    this._editor.revealLineInCenter(which.line);
  }

  private handleSelectBucket(evt) {
  }

  private handleFilterBlur(evt) {
    // this.props.onSetSearch(evt.target.value)
  }

  private handleFilterChange(evt) {
    // this.props.onSetSearch(evt.target.value)
  }
}

const mstp = (state: AppState): StateProps => {
  const plugins = [{
      name: 'cpu',
      type: 'input',
      description: 'watch your cpu yo',
      code: `
[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states.
  report_active = false
`
    }, {
      name: 'disk',
      type: 'input',
      description: 'watch your disks yo',
      code: `
[[inputs.disk]]
  ## By default stats will be gathered for all mount points.
  ## Set mount_points will restrict the stats to only the specified mount points.
  # mount_points = ["/"]
  ## Ignore mount points by filesystem type.
  ignore_fs = ["tmpfs", "devtmpfs", "devfs", "overlay", "aufs", "squashfs"]
`
    }, {
      name: 'diskio',
      type: 'input',
      description: 'watch your diskio yo',
      code: `
[[inputs.diskio]]
`
    }, {
      name: 'memory',
      type: 'input',
      description: 'watch your memory yo',
      code: `
[[inputs.mem]]
`
    }, {
      name: 'network',
      type: 'input',
      description: 'watch your network yo',
      code: `
[[inputs.net]]
`
    }, {
      name: 'system',
      type: 'bundle',
      include: [
        'cpu',
        'disk',
        'diskio',
        'memory',
        'network'
      ],
    }, {
      name: 'kubernetes',
      type: 'input',
      description: 'watch your cluster yo',
      code: `
[[inputs.kubernetes]]
  ## URL for the kubelet
  ## exp: http://1.1.1.1:10255
  url = "http://url"
`
    }, {
      name: 'agent',
      type: 'system',
      description: 'describe the agent',
      code: `
# Configuration for telegraf agent
[agent]
  ## Default data collection interval for all inputs
  interval = "10s"
  ## Rounds collection interval to 'interval'
  ## ie, if interval="10s" then always collect on :00, :10, :20, etc.
  round_interval = true

  ## Telegraf will send metrics to outputs in batches of at most
  ## metric_batch_size metrics.
  ## This controls the size of writes that Telegraf sends to output plugins.
  metric_batch_size = 1000

  ## For failed writes, telegraf will cache metric_buffer_limit metrics for each
  ## output, and will flush this buffer on a successful write. Oldest metrics
  ## are dropped first when this buffer fills.
  ## This buffer only fills when writes fail to output plugin(s).
  metric_buffer_limit = 10000

  ## Collection jitter is used to jitter the collection by a random amount.
  ## Each plugin will sleep for a random time within jitter before collecting.
  ## This can be used to avoid many plugins querying things like sysfs at the
  ## same time, which can have a measurable effect on the system.
  collection_jitter = "0s"

  ## Default flushing interval for all outputs. Maximum flush_interval will be
  ## flush_interval + flush_jitter
  flush_interval = "10s"
  ## Jitter the flush interval by a random amount. This is primarily to avoid
  ## large write spikes for users running a large number of telegraf instances.
  ## ie, a jitter of 5s and interval 10s means flushes will happen every 10-15s
  flush_jitter = "0s"

  ## By default or when set to "0s", precision will be set to the same
  ## timestamp order as the collection interval, with the maximum being 1s.
  ##   ie, when interval = "10s", precision will be "1s"
  ##       when interval = "250ms", precision will be "1ms"
  ## Precision will NOT be used for service inputs. It is up to each individual
  ## service input to set the timestamp at the appropriate precision.
  ## Valid time units are "ns", "us" (or "µs"), "ms", "s".
  precision = ""

  ## Logging configuration:
  ## Run telegraf with debug log messages.
  debug = false
  ## Run telegraf in quiet mode (error log messages only).
  quiet = false
  ## Specify the log file name. The empty string means to log to stderr.
  logfile = ""

  ## Override default hostname, if empty use os.Hostname()
  hostname = ""
  ## If set to true, do no set the "host" tag in the telegraf agent.
  omit_hostname = false
`
    }, {
      name: 'influxdb_v2',
      type: 'output',
      description: 'output to the cloud',
      code: `
[[outputs.influxdb_v2]]
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ## urls exp: http://127.0.0.1:9999
  urls = ["https://us-west-2-1.aws.cloud2.influxdata.com"]

  ## Token for authentication.
  token = "$INFLUX_TOKEN"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "aboatwright@influxdata.com"

  ## Destination bucket to write into.
  bucket = "aboatwright's Bucket"
`
    }, {
      name: '__default__',
      type: 'bundle',
      description: 'default data for a blank telegraf',
      includes: [
        'agent',
        'influxdb_v2',
      ]
    }]

  const map = plugins.reduce((prev, curr) => {
    prev[curr.name] = curr
    return prev
  }, {})
  const script = map['__default__'].includes
    .map(i => {
      return map[i].code
    })
    .join('\n')

  return {
    buckets: state.buckets.list || [],
    plugins,
    script,
    search: '',
  }
}

export {TelegrafEditor}
export default connect<StateProps, {}, {}>(
  mstp,
  null
)(TelegrafEditor)
