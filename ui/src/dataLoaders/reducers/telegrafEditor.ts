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
