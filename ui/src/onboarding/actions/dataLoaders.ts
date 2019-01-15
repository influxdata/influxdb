// Libraries
import _ from 'lodash'

// Apis
import {
  writeLineProtocol,
  createTelegrafConfig,
  getTelegrafConfigs,
  updateTelegrafConfig,
} from 'src/onboarding/apis/index'

// Utils
import {createNewPlugin} from 'src/onboarding/utils/pluginConfigs'

// Constants
import {
  pluginsByBundle,
  telegrafPluginsInfo,
} from 'src/onboarding/constants/pluginConfigs'

// Types
import {
  TelegrafPlugin,
  TelegrafPluginName,
  DataLoaderType,
  LineProtocolTab,
  Plugin,
  BundleName,
  ConfigurationState,
} from 'src/types/v2/dataLoaders'
import {AppState} from 'src/types/v2'
import {RemoteDataState} from 'src/types'
import {
  WritePrecision,
  TelegrafRequest,
  TelegrafPluginOutputInfluxDBV2,
} from 'src/api'

type GetState = () => AppState

const DEFAULT_COLLECTION_INTERVAL = 10000

export type Action =
  | SetDataLoadersType
  | SetTelegrafConfigID
  | UpdateTelegrafPluginConfig
  | AddConfigValue
  | RemoveConfigValue
  | SetActiveTelegrafPlugin
  | SetLineProtocolBody
  | SetActiveLPTab
  | SetLPStatus
  | SetPrecision
  | UpdateTelegrafPlugin
  | AddPluginBundle
  | AddTelegrafPlugins
  | RemoveBundlePlugins
  | RemovePluginBundle
  | SetPluginConfiguration
  | SetConfigArrayValue
  | SetScrapingInterval
  | SetScrapingBucket
  | AddScrapingURL
  | RemoveScrapingURL
  | UpdateScrapingURL

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

interface SetConfigArrayValue {
  type: 'SET_TELEGRAF_PLUGIN_CONFIG_VALUE'
  payload: {
    pluginName: TelegrafPluginName
    field: string
    valueIndex: number
    value: string
  }
}

export const setConfigArrayValue = (
  pluginName: TelegrafPluginName,
  field: string,
  valueIndex: number,
  value: string
): SetConfigArrayValue => ({
  type: 'SET_TELEGRAF_PLUGIN_CONFIG_VALUE',
  payload: {pluginName, field, valueIndex, value},
})

interface SetTelegrafConfigID {
  type: 'SET_TELEGRAF_CONFIG_ID'
  payload: {id: string}
}

export const setTelegrafConfigID = (id: string): SetTelegrafConfigID => ({
  type: 'SET_TELEGRAF_CONFIG_ID',
  payload: {id},
})

interface AddPluginBundle {
  type: 'ADD_PLUGIN_BUNDLE'
  payload: {bundle: BundleName}
}

export const addPluginBundle = (bundle: BundleName): AddPluginBundle => ({
  type: 'ADD_PLUGIN_BUNDLE',
  payload: {bundle},
})

interface RemovePluginBundle {
  type: 'REMOVE_PLUGIN_BUNDLE'
  payload: {bundle: BundleName}
}

export const removePluginBundle = (bundle: BundleName): RemovePluginBundle => ({
  type: 'REMOVE_PLUGIN_BUNDLE',
  payload: {bundle},
})
interface AddTelegrafPlugins {
  type: 'ADD_TELEGRAF_PLUGINS'
  payload: {telegrafPlugins: TelegrafPlugin[]}
}

export const addTelegrafPlugins = (
  telegrafPlugins: TelegrafPlugin[]
): AddTelegrafPlugins => ({
  type: 'ADD_TELEGRAF_PLUGINS',
  payload: {telegrafPlugins},
})

interface RemoveBundlePlugins {
  type: 'REMOVE_BUNDLE_PLUGINS'
  payload: {bundle: BundleName}
}

export const removeBundlePlugins = (
  bundle: BundleName
): RemoveBundlePlugins => ({
  type: 'REMOVE_BUNDLE_PLUGINS',
  payload: {bundle},
})

interface SetScrapingInterval {
  type: 'SET_SCRAPING_INTERVAL'
  payload: {interval: string}
}

export const setScrapingInterval = (interval: string): SetScrapingInterval => ({
  type: 'SET_SCRAPING_INTERVAL',
  payload: {interval},
})

interface SetScrapingBucket {
  type: 'SET_SCRAPING_BUCKET'
  payload: {bucket: string}
}

export const setScrapingBucket = (bucket: string): SetScrapingBucket => ({
  type: 'SET_SCRAPING_BUCKET',
  payload: {bucket},
})

interface AddScrapingURL {
  type: 'ADD_SCRAPING_URL'
  payload: {url: string}
}

export const addScrapingURL = (url: string): AddScrapingURL => ({
  type: 'ADD_SCRAPING_URL',
  payload: {url},
})

interface RemoveScrapingURL {
  type: 'REMOVE_SCRAPING_URL'
  payload: {url: string}
}

export const removeScrapingURL = (url: string): RemoveScrapingURL => ({
  type: 'REMOVE_SCRAPING_URL',
  payload: {url},
})

interface UpdateScrapingURL {
  type: 'UPDATE_SCRAPING_URL'
  payload: {index: number; url: string}
}

export const updateScrapingURL = (
  index: number,
  url: string
): UpdateScrapingURL => ({
  type: 'UPDATE_SCRAPING_URL',
  payload: {index, url},
})

export const addPluginBundleWithPlugins = (bundle: BundleName) => dispatch => {
  dispatch(addPluginBundle(bundle))
  const plugins = pluginsByBundle[bundle]
  dispatch(
    addTelegrafPlugins(
      plugins.map(p => {
        const isConfigured = !!telegrafPluginsInfo[p].fields
          ? ConfigurationState.Unconfigured
          : ConfigurationState.Configured

        return {
          name: p,
          active: false,
          configured: isConfigured,
        }
      })
    )
  )
}

export const removePluginBundleWithPlugins = (
  bundle: BundleName
) => dispatch => {
  dispatch(removePluginBundle(bundle))
  dispatch(removeBundlePlugins(bundle))
}

export const createOrUpdateTelegrafConfigAsync = (authToken: string) => async (
  dispatch,
  getState: GetState
) => {
  const {
    onboarding: {
      dataLoaders: {telegrafPlugins},
      steps: {
        setupParams: {org, bucket},
        orgID,
      },
    },
  } = getState()

  const telegrafConfigsFromServer = await getTelegrafConfigs(orgID)

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

  let plugins: Plugin[] = [influxDB2Out]
  telegrafPlugins.forEach(tp => {
    if (tp.configured === ConfigurationState.Configured) {
      plugins = [...plugins, tp.plugin || createNewPlugin(tp.name)]
    }
  })

  const telegrafRequest: TelegrafRequest = {
    name: 'new config',
    agent: {collectionInterval: DEFAULT_COLLECTION_INTERVAL},
    organizationID: orgID,
    plugins,
  }

  if (telegrafConfigsFromServer.length) {
    const id = _.get(telegrafConfigsFromServer, '0.id', '')

    await updateTelegrafConfig(id, telegrafRequest)
    dispatch(setTelegrafConfigID(id))
    return
  }

  const created = await createTelegrafConfig(telegrafRequest)
  dispatch(setTelegrafConfigID(created.id))
}

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

interface SetPluginConfiguration {
  type: 'SET_PLUGIN_CONFIGURATION_STATE'
  payload: {telegrafPlugin: TelegrafPluginName}
}

export const setPluginConfiguration = (
  telegrafPlugin: TelegrafPluginName
): SetPluginConfiguration => ({
  type: 'SET_PLUGIN_CONFIGURATION_STATE',
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
