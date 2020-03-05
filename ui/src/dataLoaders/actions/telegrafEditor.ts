import {Dispatch} from 'react'

import {getTelegrafPlugins} from 'src/client'

import {
  TelegrafEditorPluginState,
  TelegrafEditorActivePluginState,
  TelegrafEditorBasicPlugin,
} from 'src/dataLoaders/reducers/telegrafEditor'

// Utils

// Types
import {Bucket, RemoteDataState, ResourceType, GetState} from 'src/types'
import {getStatus} from 'src/resources/selectors'

export type PluginResourceAction =
  | ReturnType<typeof setPlugins>
  | ReturnType<typeof setPluginLoadingState>

export const getPlugins = () => async (
  dispatch: Dispatch<PluginResourceAction>,
  getState: GetState
) => {
  const state = getState()
  if (getStatus(state, ResourceType.Plugins) === RemoteDataState.NotStarted) {
    dispatch(setPluginLoadingState(RemoteDataState.Loading))
  }

  dispatch(setPluginLoadingState(RemoteDataState.Loading))

  const result = await getTelegrafPlugins({}, {})

  if (result.status === 200) {
    const plugins = result.data.plugins as TelegrafEditorBasicPlugin[]

    dispatch(setPlugins(plugins))
  }

  dispatch(setPluginLoadingState(RemoteDataState.Done))
}

export const setPluginLoadingState = (state: RemoteDataState) => ({
  type: 'SET_TELEGRAF_EDITOR_PLUGINS_LOADING_STATE' as 'SET_TELEGRAF_EDITOR_PLUGINS_LOADING_STATE',
  payload: state,
})

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
  | ReturnType<typeof setLookup>
  | ReturnType<typeof setList>
  | ReturnType<typeof setText>
  | ReturnType<typeof setBucket>
  | ReturnType<typeof setFilter>
  | ReturnType<typeof reset>

export const setLookup = (show: boolean) => ({
  type: 'SET_TELEGRAF_EDITOR_LOOKUP' as 'SET_TELEGRAF_EDITOR_LOOKUP',
  payload: show,
})

export const setList = (show: boolean) => ({
  type: 'SET_TELEGRAF_EDITOR_LIST' as 'SET_TELEGRAF_EDITOR_LIST',
  payload: show,
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
