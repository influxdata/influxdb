// Libraries
import _ from 'lodash'

// Apis
import {
  createTelegrafConfig,
  updateTelegrafConfig,
  createScraperTarget,
  updateScraperTarget,
  getTelegrafConfig,
} from 'src/onboarding/apis/index'
import {client} from 'src/utils/api'

// Utils
import {createNewPlugin} from 'src/dataLoaders/utils/pluginConfigs'

// Constants
import {
  pluginsByBundle,
  telegrafPluginsInfo,
} from 'src/dataLoaders/constants/pluginConfigs'

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
import {Dispatch} from 'redux'

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
  | SetScraperTargetBucket
  | SetScraperTargetURL
  | SetScraperTargetID
  | ClearDataLoaders
  | SetTelegrafConfigName

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

interface ClearDataLoaders {
  type: 'CLEAR_DATA_LOADERS'
}

export const clearDataLoaders = (): ClearDataLoaders => ({
  type: 'CLEAR_DATA_LOADERS',
})

interface SetTelegrafConfigName {
  type: 'SET_TELEGRAF_CONFIG_NAME'
  payload: {name: string}
}

export const setTelegrafConfigName = (name: string): SetTelegrafConfigName => ({
  type: 'SET_TELEGRAF_CONFIG_NAME',
  payload: {name},
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

interface SetScraperTargetBucket {
  type: 'SET_SCRAPER_TARGET_BUCKET'
  payload: {bucket: string}
}

export const setScraperTargetBucket = (
  bucket: string
): SetScraperTargetBucket => ({
  type: 'SET_SCRAPER_TARGET_BUCKET',
  payload: {bucket},
})

interface SetScraperTargetURL {
  type: 'SET_SCRAPER_TARGET_URL'
  payload: {url: string}
}

export const setScraperTargetURL = (url: string): SetScraperTargetURL => ({
  type: 'SET_SCRAPER_TARGET_URL',
  payload: {url},
})

interface SetScraperTargetID {
  type: 'SET_SCRAPER_TARGET_ID'
  payload: {id: string}
}

export const setScraperTargetID = (id: string): SetScraperTargetID => ({
  type: 'SET_SCRAPER_TARGET_ID',
  payload: {id},
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
    dataLoading: {
      dataLoaders: {telegrafPlugins, telegrafConfigID, telegrafConfigName},
      steps: {org, bucket, orgID},
    },
  } = getState()

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

  if (telegrafConfigID) {
    const telegrafConfig = await getTelegrafConfig(telegrafConfigID)
    const id = _.get(telegrafConfig, 'id', '')

    await updateTelegrafConfig(id, {
      ...telegrafConfig,
      name: telegrafConfigName,
      plugins,
    })
    dispatch(setTelegrafConfigID(id))
    return
  }

  const telegrafRequest: TelegrafRequest = {
    name: telegrafConfigName,
    agent: {collectionInterval: DEFAULT_COLLECTION_INTERVAL},
    organizationID: orgID,
    plugins,
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
    await client.write.create(org, bucket, body, {precision})
    dispatch(setLPStatus(RemoteDataState.Done))
  } catch (error) {
    dispatch(setLPStatus(RemoteDataState.Error))
  }
}

export const saveScraperTarget = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const {
    dataLoading: {
      dataLoaders: {
        scraperTarget: {url, id},
      },
      steps: {bucketID, orgID},
    },
  } = getState()

  try {
    if (id) {
      await updateScraperTarget(id, url, bucketID)
    } else {
      const newTarget = await createScraperTarget(url, orgID, bucketID)
      dispatch(setScraperTargetID(newTarget.id))
    }
  } catch (error) {
    console.error()
  }
}
