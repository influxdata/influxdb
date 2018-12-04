// Types
import {TelegrafPlugin, DataLoaderType} from 'src/types/v2/dataLoaders'

export type Action =
  | SetDataLoadersType
  | AddTelegrafPlugin
  | RemoveTelegrafPlugin
  | SetActiveTelegrafPlugin

interface SetDataLoadersType {
  type: 'SET_DATA_LOADERS_TYPE'
  payload: {type: DataLoaderType}
}

export const setDataLoadersType = (
  type: DataLoaderType
): SetDataLoadersType => ({
  type: 'SET_DATA_LOADERS_TYPE',
  payload: {type},
})

interface AddTelegrafPlugin {
  type: 'ADD_TELEGRAF_PLUGIN'
  payload: {telegrafPlugin: TelegrafPlugin}
}

export const addTelegrafPlugin = (
  telegrafPlugin: TelegrafPlugin
): AddTelegrafPlugin => ({
  type: 'ADD_TELEGRAF_PLUGIN',
  payload: {telegrafPlugin},
})

interface RemoveTelegrafPlugin {
  type: 'REMOVE_TELEGRAF_PLUGIN'
  payload: {telegrafPlugin: string}
}

export const removeTelegrafPlugin = (
  telegrafPlugin: string
): RemoveTelegrafPlugin => ({
  type: 'REMOVE_TELEGRAF_PLUGIN',
  payload: {telegrafPlugin},
})

interface SetActiveTelegrafPlugin {
  type: 'SET_ACTIVE_TELEGRAF_PLUGIN'
  payload: {telegrafPlugin: string}
}

export const setActiveTelegrafPlugin = (
  telegrafPlugin: string
): SetActiveTelegrafPlugin => ({
  type: 'SET_ACTIVE_TELEGRAF_PLUGIN',
  payload: {telegrafPlugin},
})
