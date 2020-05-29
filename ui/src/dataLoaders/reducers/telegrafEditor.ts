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
  showLookup: boolean
  showList: boolean
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
    name: '__default__',
    type: 'bundle',
    description: 'default data for a blank telegraf',
    include: ['agent', 'influxdb_v2'],
  },
]

const INITIAL_EDITOR: TelegrafEditorState = {
  showLookup: true,
  showList: true,
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
      const plugs = INITIAL_PLUGINS.slice(0)
      plugs.push(...action.payload.slice(0))
      return plugs
    default:
      return state
  }
}

export interface PluginResourceState {
  status: RemoteDataState
}

export function pluginsResourceReducer(
  state = {status: RemoteDataState.NotStarted},
  action: PluginResourceAction
): PluginResourceState {
  switch (action.type) {
    case 'SET_TELEGRAF_EDITOR_PLUGINS_LOADING_STATE':
      return {...state, status: action.payload}
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
    case 'RESET_TELEGRAF_EDITOR':
      return {...INITIAL_EDITOR}
    default:
      return state
  }
}
