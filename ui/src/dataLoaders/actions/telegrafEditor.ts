import {Bucket, RemoteDataState} from 'src/types'
import {getTelegrafPlugins} from 'src/client'
import {Dispatch} from 'react'
import {client} from 'src/utils/api'
import {
  PublishNotificationAction,
  notify,
} from 'src/shared/actions/notifications'
import {getTelegrafConfigFailed} from 'src/shared/copy/notifications'
import {
  TelegrafEditorPluginState,
  TelegrafEditorActivePluginState,
  TelegrafEditorBasicPlugin,
} from 'src/dataLoaders/reducers/telegrafEditor'

export type PluginResourceAction =
  | ReturnType<typeof setPlugins>
  | ReturnType<typeof setPluginLoadingState>
  | ReturnType<typeof setSaveState>
  | ReturnType<typeof setLoadState>

export const getPlugins = () => async (
  dispatch: Dispatch<PluginResourceAction>
) => {
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

export const setLoadState = (state: RemoteDataState) => ({
  type: 'SET_TELEGRAF_EDITOR_LOAD_STATE' as 'SET_TELEGRAF_EDITOR_LOAD_STATE',
  payload: state,
})

export const setSaveState = (state: RemoteDataState) => ({
  type: 'SET_TELEGRAF_EDITOR_SAVE_STATE' as 'SET_TELEGRAF_EDITOR_SAVE_STATE',
  payload: state,
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
  | ReturnType<typeof setName>
  | ReturnType<typeof setDescription>
  | ReturnType<typeof setSetup>
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

export const setName = (name: string) => ({
  type: 'SET_TELEGRAF_EDITOR_NAME' as 'SET_TELEGRAF_EDITOR_NAME',
  payload: name,
})

export const setDescription = (description: string) => ({
  type: 'SET_TELEGRAF_EDITOR_DESCRIPTION' as 'SET_TELEGRAF_EDITOR_DESCRIPTION',
  payload: description,
})

export const setSetup = (show: boolean) => ({
  type: 'SET_TELEGRAF_EDITOR_SETUP' as 'SET_TELEGRAF_EDITOR_SETUP',
  payload: show,
})

export const reset = () => ({
  type: 'RESET_TELEGRAF_EDITOR' as 'RESET_TELEGRAF_EDITOR',
})

export type LoadTelegrafConfig =
  | PluginResourceAction
  | EditorAction
  | ReturnType<typeof getPlugins>
  | PublishNotificationAction

export const loadTelegrafConfig = (id: string) => (
  dispatch: Dispatch<LoadTelegrafConfig>
) => {
  try {
    dispatch(setLoadState(RemoteDataState.Loading))
    Promise.all([
      client.telegrafConfigs.get(id),
      client.telegrafConfigs.getTOML(id),
    ]).then(resps => {
      dispatch(setText(resps[1]))
      dispatch(setName(resps[0].name))
      dispatch(setDescription(resps[0].description))
      dispatch(getPlugins())
      dispatch(setLoadState(RemoteDataState.Done))
    })
  } catch (error) {
    dispatch(setLoadState(RemoteDataState.Error))
    dispatch(notify(getTelegrafConfigFailed()))
  }
}

export type SaveTelegrafConfig = PluginResourceAction | EditorAction

export const saveTelegrafConfig = () => (
  dispatch: Dispatch<SaveTelegrafConfig>
) => {
  dispatch(setSaveState(RemoteDataState.Loading))

  setTimeout(() => {
    dispatch(setSetup(true))
    dispatch(setSaveState(RemoteDataState.Done))
  }, 200)
}
