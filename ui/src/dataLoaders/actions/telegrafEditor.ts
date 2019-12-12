import {Bucket} from 'src/types'
import {
  TelegrafEditorPluginState,
  TelegrafEditorActivePluginState,
} from 'src/dataLoaders/reducers/telegrafEditor'

export type PluginAction = ReturnType<typeof setPlugins>

export const setPlugins = (plugins: TelegrafEditorPluginState) => ({
  type: 'SET_TELEGRAF_EDITOR_PLUGINS' as 'SET_TELEGRAF_EDITOR_PLUGINS',
  payload: plugins,
})

export type ActivePluginAction = ReturnType<typeof setActivePlugins>

export const setActivePlugins = (plugins: TelegrafEditorActivePluginState) => ({
  type: 'SET_TELEGRAF_EDITOR_ACTIVE_PLUGINS' as 'SET_TELEGRAF_EDITOR_ACTIVE_PLUGINS',
  payload: plugins,
})

export type EditorAction =
  | ReturnType<typeof setMode>
  | ReturnType<typeof setText>
  | ReturnType<typeof setBucket>
  | ReturnType<typeof setFilter>
  | ReturnType<typeof reset>

export const setMode = (mode: 'adding' | 'indexing') => ({
  type: 'SET_TELEGRAF_EDITOR_MODE' as 'SET_TELEGRAF_EDITOR_MODE',
  payload: mode,
})

export const setText = (text: string) => ({
  type: 'SET_TELEGRAF_EDITOR_TEXT' as 'SET_TELEGRAF_EDITOR_TEXT',
  payload: text,
})

export const setBucket = (bucket: Bucket) => ({
  type: 'SET_TELEGRAF_EDITOR_ACTIVE_BUCKET' as 'SET_TELEGRAF_EDITOR_ACTIVE_BUCKET',
  payload: bucket,
})

export const setFilter = (filter: string) => ({
  type: 'SET_TELEGRAF_EDITOR_FILTER' as 'SET_TELEGRAF_EDITOR_FILTER',
  payload: filter,
})

export const reset = () => ({
  type: 'RESET_TELEGRAF_EDITOR' as 'RESET_TELEGRAF_EDITOR',
})
