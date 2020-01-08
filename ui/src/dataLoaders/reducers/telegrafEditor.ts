import {Bucket, RemoteDataState} from 'src/types'
import {TelegrafPlugin} from 'src/client'
import {
  PluginResourceAction,
  ActivePluginAction,
  EditorAction,
} from 'src/dataLoaders/actions/telegrafEditor'

export type TelegrafEditorPluginType =
  | 'system'
  | 'input'
  | 'output'
  | 'processor'
  | 'aggregator'
  | 'display'
type TelegrafEditorPluginName = string

export type TelegrafEditorBasicPlugin = TelegrafPlugin & {
  type: TelegrafEditorPluginType
}
export interface TelegrafEditorBundlePlugin {
  name: TelegrafEditorPluginName
  description: string
  type: 'bundle'
  include: Array<TelegrafEditorPluginName>
}

export type TelegrafEditorPlugin =
  | TelegrafEditorBasicPlugin
  | TelegrafEditorBundlePlugin
export type TelegrafEditorPluginState = Array<TelegrafEditorPlugin>

export interface TelegrafEditorActivePlugin {
  name: string
  type: TelegrafEditorPluginType
  line: number
}

export type TelegrafEditorActivePluginState = Array<TelegrafEditorActivePlugin>

export interface TelegrafEditorState {
  id: string
  name: string
  description: string
  showLookup: boolean
  showList: boolean
  showSetup: boolean
  bucket: Bucket | null
  text: string
  filter: string
}

const INITIAL_PLUGINS: TelegrafEditorPluginState = [
  {
    name: 'system',
    type: 'bundle',
    description: 'collect all the basic local metrics',
    include: ['cpu', 'disk', 'diskio', 'memory', 'network'],
  },
  {
    name: 'agent',
    type: 'system',
    description: 'configures Telegraf and the defaults used across all plugins',
    config: `[agent]
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
`,
  },
  {
    name: 'influxdb_v2',
    type: 'output',
    description: 'Configuration for sending metrics to InfluxDB',
    config: `# Configuration for sending metrics to InfluxDB
[[outputs.influxdb_v2]]
  # alias="influxdb_v2"
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ##   ex: urls = ["https://us-west-2-1.aws.cloud2.influxdata.com"]
  urls = ["<%= server %>"]

  ## Token for authentication.
  token = "<%= token %>"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "<%= org %>"

  ## Destination bucket to write into.
  bucket = "<%= bucket %>"

  ## The value of this tag will be used to determine the bucket.  If this
  ## tag is not set the 'bucket' option is used as the default.
  # bucket_tag = ""

  ## If true, the bucket tag will not be added to the metric.
  # exclude_bucket_tag = false

  ## Timeout for HTTP messages.
  # timeout = "5s"

  ## Additional HTTP headers
  # http_headers = {"X-Special-Header" = "Special-Value"}

  ## HTTP Proxy override, if unset values the standard proxy environment
  ## variables are consulted to determine which proxy, if any, should be used.
  # http_proxy = "http://corporate.proxy:3128"

  ## HTTP User-Agent
  # user_agent = "telegraf"

  ## Content-Encoding for write request body, can be set to "gzip" to
  ## compress body or "identity" to apply no encoding.
  # content_encoding = "gzip"

  ## Enable or disable uint support for writing uints influxdb 2.0.
  # influx_uint_support = false

  ## Optional TLS Config for use on HTTP connections.
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`,
  },
  {
    name: '__default__',
    type: 'bundle',
    description: 'default data for a blank telegraf',
    include: ['agent', 'influxdb_v2'],
  },
]

const INITIAL_EDITOR: TelegrafEditorState = {
  id: '',
  name: 'Name this Configuration',
  description: '',
  showLookup: false,
  showList: true,
  showSetup: false,
  bucket: null,
  text: '',
  filter: '',
}

export function pluginsReducer(
  state = INITIAL_PLUGINS,
  action: PluginResourceAction
): TelegrafEditorPluginState {
  switch (action.type) {
    case 'SET_TELEGRAF_EDITOR_PLUGINS':
      const map = (arr, prev = {}) => {
        return arr.reduce((prev, curr) => {
          const key = curr.type + '_' + curr.name

          if (!prev.hasOwnProperty(key)) {
            prev[key] = curr
          }

          return prev
        }, prev)
      }

      const reduced = map(
        action.payload.slice(0),
        map(INITIAL_PLUGINS.slice(0))
      )
      return Object.keys(reduced).map(k => reduced[k])
    default:
      return state
  }
}

export interface PluginResourceState {
  status: RemoteDataState
  loadStatus: RemoteDataState
  saveStatus: RemoteDataState
}

export function pluginsResourceReducer(
  state = {
    status: RemoteDataState.NotStarted,
    loadStatus: RemoteDataState.NotStarted,
    saveStatus: RemoteDataState.NotStarted,
  },
  action: PluginResourceAction
): PluginResourceState {
  switch (action.type) {
    case 'SET_TELEGRAF_EDITOR_PLUGINS_LOADING_STATE':
      return {...state, status: action.payload}
    case 'SET_TELEGRAF_EDITOR_LOAD_STATE':
      return {...state, loadStatus: action.payload}
    case 'SET_TELEGRAF_EDITOR_SAVE_STATE':
      return {...state, saveStatus: action.payload}
    default:
      return state
  }
}

export function activePluginsReducer(
  state: TelegrafEditorActivePluginState = [],
  action: ActivePluginAction
): TelegrafEditorActivePluginState {
  switch (action.type) {
    case 'SET_TELEGRAF_EDITOR_ACTIVE_PLUGINS':
      return action.payload.slice(0)
    default:
      return state
  }
}

export function editorReducer(
  state = INITIAL_EDITOR,
  action: EditorAction
): TelegrafEditorState {
  switch (action.type) {
    case 'SET_TELEGRAF_EDITOR_LOOKUP':
      return {...state, showLookup: action.payload}
    case 'SET_TELEGRAF_EDITOR_LIST':
      return {...state, showList: action.payload}
    case 'SET_TELEGRAF_EDITOR_TEXT':
      return {...state, text: action.payload}
    case 'SET_TELEGRAF_EDITOR_ACTIVE_BUCKET':
      return {...state, bucket: action.payload}
    case 'SET_TELEGRAF_EDITOR_FILTER':
      return {...state, filter: action.payload}
    case 'SET_TELEGRAF_EDITOR_NAME':
      return {...state, name: action.payload}
    case 'SET_TELEGRAF_EDITOR_DESCRIPTION':
      return {...state, description: action.payload}
    case 'SET_TELEGRAF_EDITOR_SETUP':
      return {...state, showSetup: action.payload}
    case 'RESET_TELEGRAF_EDITOR':
      return {...INITIAL_EDITOR}
    default:
      return state
  }
}
