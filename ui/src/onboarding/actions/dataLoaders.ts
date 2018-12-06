// Types
import {
  TelegrafPlugin,
  DataLoaderType,
  LineProtocolTab,
} from 'src/types/v2/dataLoaders'

import {notify} from 'src/shared/actions/notifications'
import {writeLineProtocolFailed} from 'src/shared/copy/v2/notifications'
import {writeLineProtocol} from 'src/onboarding/apis/index'

export type Action =
  | SetDataLoadersType
  | AddTelegrafPlugin
  | RemoveTelegrafPlugin
  | SetActiveTelegrafPlugin
  | SetLineProtocolText
  | SetActiveLPTab

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

interface SetLineProtocolText {
  type: 'SET_LINE_PROTOCOL_TEXT'
  payload: {lineProtocolText: string}
}

export const setLineProtocolText = (
  lineProtocolText: string
): SetLineProtocolText => ({
  type: 'SET_LINE_PROTOCOL_TEXT',
  payload: {lineProtocolText},
})

interface SetActiveLPTab {
  type: 'SET_ACTIVE_LP_TAB'
  payload: {activeLPTab: LineProtocolTab}
}

export const setActiveLPTab = (
  activeLPTab: LineProtocolTab
): SetActiveLPTab => ({
  type: 'SET_ACTIVE_LP_TAB',
  payload: {activeLPTab},
})

export const writeLineProtocolAction = (
  org: string,
  bucket: string,
  body: string
) => async dispatch => {
  try {
    await writeLineProtocol(org, bucket, body)
  } catch (error) {
    dispatch(notify(writeLineProtocolFailed(error)))
  }
}
