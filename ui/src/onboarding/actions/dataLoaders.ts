// Libraries
import _ from 'lodash'

// Apis
import {writeLineProtocol} from 'src/onboarding/apis/index'
import {telegrafsAPI} from 'src/utils/api'

// Utils
import {createNewPlugin} from 'src/onboarding/utils/pluginConfigs'

// Types
import {
  TelegrafPlugin,
  DataLoaderType,
  LineProtocolTab,
  Plugin,
} from 'src/types/v2/dataLoaders'
import {AppState} from 'src/types/v2'
import {RemoteDataState} from 'src/types'
import {WritePrecision} from 'src/api'
import {TelegrafPluginOutputInfluxDBV2} from 'src/api'

type GetState = () => AppState

const DEFAULT_COLLECTION_INTERVAL = 15

export type Action =
  | SetDataLoadersType
  | SetTelegrafConfigID
  | AddTelegrafPlugin
  | UpdateTelegrafPluginConfig
  | AddConfigValue
  | RemoveConfigValue
  | RemoveTelegrafPlugin
  | SetActiveTelegrafPlugin
  | SetLineProtocolBody
  | SetActiveLPTab
  | SetLPStatus
  | SetPrecision
  | UpdateTelegrafPlugin

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

interface UpdateTelegrafPluginConfig {
  type: 'UPDATE_TELEGRAF_PLUGIN_CONFIG'
  payload: {name: string; field: string; value: string}
}

export const updateTelegrafPluginConfig = (
  name: string,
  field: string,
  value: string
): UpdateTelegrafPluginConfig => ({
  type: 'UPDATE_TELEGRAF_PLUGIN_CONFIG',
  payload: {name, field, value},
})

interface UpdateTelegrafPlugin {
  type: 'UPDATE_TELEGRAF_PLUGIN'
  payload: {plugin: Plugin}
}

export const updateTelegrafPlugin = (plugin: Plugin): UpdateTelegrafPlugin => ({
  type: 'UPDATE_TELEGRAF_PLUGIN',
  payload: {plugin},
})

interface AddConfigValue {
  type: 'ADD_TELEGRAF_PLUGIN_CONFIG_VALUE'
  payload: {
    pluginName: string
    fieldName: string
    value: string
  }
}

export const addConfigValue = (
  pluginName: string,
  fieldName: string,
  value: string
): AddConfigValue => ({
  type: 'ADD_TELEGRAF_PLUGIN_CONFIG_VALUE',
  payload: {pluginName, fieldName, value},
})

interface RemoveConfigValue {
  type: 'REMOVE_TELEGRAF_PLUGIN_CONFIG_VALUE'
  payload: {
    pluginName: string
    fieldName: string
    value: string
  }
}

export const removeConfigValue = (
  pluginName: string,
  fieldName: string,
  value: string
): RemoveConfigValue => ({
  type: 'REMOVE_TELEGRAF_PLUGIN_CONFIG_VALUE',
  payload: {pluginName, fieldName, value},
})

interface SetTelegrafConfigID {
  type: 'SET_TELEGRAF_CONFIG_ID'
  payload: {id: string}
}

export const setTelegrafConfigID = (id: string): SetTelegrafConfigID => ({
  type: 'SET_TELEGRAF_CONFIG_ID',
  payload: {id},
})

export const createTelegrafConfigAsync = (authToken: string) => async (
  dispatch,
  getState: GetState
) => {
  const {
    onboarding: {
      dataLoaders: {telegrafPlugins},
      steps: {
        setupParams: {org, bucket},
      },
    },
  } = getState()

  let plugins = telegrafPlugins.map(tp => tp.plugin || createNewPlugin(tp.name))

  const influxDB2Out = {
    name: TelegrafPluginOutputInfluxDBV2.NameEnum.InfluxdbV2,
    type: TelegrafPluginOutputInfluxDBV2.TypeEnum.Output,
    config: {
      urls: ['http://127.0.0.1:9999'],
      token: authToken,
      organization: org,
      bucket,
    },
  }

  plugins = [...plugins, influxDB2Out]

  const body = {
    name: 'new config',
    agent: {collectionInterval: DEFAULT_COLLECTION_INTERVAL},
    plugins,
  }
  const created = await telegrafsAPI.telegrafsPost(org, body)
  dispatch(setTelegrafConfigID(created.data.id))
}

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

interface SetLineProtocolBody {
  type: 'SET_LINE_PROTOCOL_BODY'
  payload: {lineProtocolBody: string}
}

export const setLineProtocolBody = (
  lineProtocolBody: string
): SetLineProtocolBody => ({
  type: 'SET_LINE_PROTOCOL_BODY',
  payload: {lineProtocolBody},
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

interface SetLPStatus {
  type: 'SET_LP_STATUS'
  payload: {lpStatus: RemoteDataState}
}

export const setLPStatus = (lpStatus: RemoteDataState): SetLPStatus => ({
  type: 'SET_LP_STATUS',
  payload: {lpStatus},
})

interface SetPrecision {
  type: 'SET_PRECISION'
  payload: {precision: WritePrecision}
}

export const setPrecision = (precision: WritePrecision): SetPrecision => ({
  type: 'SET_PRECISION',
  payload: {precision},
})

export const writeLineProtocolAction = (
  org: string,
  bucket: string,
  body: string,
  precision: WritePrecision
) => async dispatch => {
  try {
    dispatch(setLPStatus(RemoteDataState.Loading))
    await writeLineProtocol(org, bucket, body, precision)
    dispatch(setLPStatus(RemoteDataState.Done))
  } catch (error) {
    dispatch(setLPStatus(RemoteDataState.Error))
  }
}
