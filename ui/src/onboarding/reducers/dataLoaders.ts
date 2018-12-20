// Libraries
import _ from 'lodash'

// Utils
import {
  createNewPlugin,
  updateConfigFields,
  isPluginInBundle,
  isPluginUniqueToBundle,
  getConfigFields,
} from 'src/onboarding/utils/pluginConfigs'
import {getDeep} from 'src/utils/wrappers'
import {validateURI} from 'src/shared/utils/validateURI'

// Types
import {Action} from 'src/onboarding/actions/dataLoaders'
import {
  DataLoaderType,
  LineProtocolTab,
  DataLoadersState,
  ConfigurationState,
  ConfigFieldType,
  Plugin,
} from 'src/types/v2/dataLoaders'
import {RemoteDataState} from 'src/types'
import {WritePrecision} from 'src/api'

export const INITIAL_STATE: DataLoadersState = {
  telegrafPlugins: [],
  type: DataLoaderType.Empty,
  lineProtocolBody: '',
  activeLPTab: LineProtocolTab.UploadFile,
  lpStatus: RemoteDataState.NotStarted,
  precision: WritePrecision.Ns,
  telegrafConfigID: null,
  pluginBundles: [],
}

export default (state = INITIAL_STATE, action: Action): DataLoadersState => {
  switch (action.type) {
    case 'SET_DATA_LOADERS_TYPE':
      return {
        ...state,
        type: action.payload.type,
      }
    case 'SET_TELEGRAF_CONFIG_ID':
      return {
        ...state,
        telegrafConfigID: action.payload.id,
      }
    case 'ADD_PLUGIN_BUNDLE':
      return {
        ...state,
        pluginBundles: [...state.pluginBundles, action.payload.bundle],
      }
    case 'REMOVE_PLUGIN_BUNDLE':
      return {
        ...state,
        pluginBundles: state.pluginBundles.filter(
          b => b !== action.payload.bundle
        ),
      }
    case 'REMOVE_BUNDLE_PLUGINS':
      return {
        ...state,
        telegrafPlugins: state.telegrafPlugins.filter(tp => {
          if (isPluginInBundle(tp.name, action.payload.bundle)) {
            return !isPluginUniqueToBundle(
              tp.name,
              action.payload.bundle,
              state.pluginBundles
            )
          }

          return true
        }),
      }
    case 'ADD_TELEGRAF_PLUGINS':
      return {
        ...state,
        telegrafPlugins: _.sortBy(
          _.uniqBy(
            [...state.telegrafPlugins, ...action.payload.telegrafPlugins],
            'name'
          ),
          ['name']
        ),
      }
    case 'UPDATE_TELEGRAF_PLUGIN':
      return {
        ...state,
        telegrafPlugins: state.telegrafPlugins.map(tp => {
          if (tp.name === action.payload.plugin.name) {
            return {
              ...tp,
              plugin: action.payload.plugin,
            }
          }

          return tp
        }),
      }
    case 'UPDATE_TELEGRAF_PLUGIN_CONFIG':
      return {
        ...state,
        telegrafPlugins: state.telegrafPlugins.map(tp => {
          if (tp.name === action.payload.name) {
            const plugin = _.get(tp, 'plugin', createNewPlugin(tp.name))

            return {
              ...tp,
              plugin: updateConfigFields(
                plugin,
                action.payload.field,
                action.payload.value
              ),
            }
          }
          return tp
        }),
      }
    case 'ADD_TELEGRAF_PLUGIN_CONFIG_VALUE':
      return {
        ...state,
        telegrafPlugins: state.telegrafPlugins.map(tp => {
          if (tp.name === action.payload.pluginName) {
            const plugin = _.get(tp, 'plugin', createNewPlugin(tp.name))

            const updatedConfigFieldValue: string[] = [
              ...plugin.config[action.payload.fieldName],
              action.payload.value,
            ]

            return {
              ...tp,
              plugin: updateConfigFields(
                plugin,
                action.payload.fieldName,
                updatedConfigFieldValue
              ),
            }
          }
          return tp
        }),
      }
    case 'REMOVE_TELEGRAF_PLUGIN_CONFIG_VALUE':
      return {
        ...state,
        telegrafPlugins: state.telegrafPlugins.map(tp => {
          if (tp.name === action.payload.pluginName) {
            const plugin = _.get(tp, 'plugin', createNewPlugin(tp.name))

            const configFieldValues = _.get(
              plugin,
              `config.${action.payload.fieldName}`,
              []
            )
            const filteredConfigFieldValue = configFieldValues.filter(
              v => v !== action.payload.value
            )

            return {
              ...tp,
              plugin: updateConfigFields(
                plugin,
                action.payload.fieldName,
                filteredConfigFieldValue
              ),
            }
          }
          return tp
        }),
      }
    case 'SET_TELEGRAF_PLUGIN_CONFIG_VALUE':
      return {
        ...state,
        telegrafPlugins: state.telegrafPlugins.map(tp => {
          if (tp.name === action.payload.pluginName) {
            const plugin = _.get(tp, 'plugin', createNewPlugin(tp.name))
            const configValues = _.get(
              plugin,
              `config.${action.payload.field}`,
              []
            )
            configValues[action.payload.valueIndex] = action.payload.value
            return {
              ...tp,
              plugin: updateConfigFields(plugin, action.payload.field, [
                ...configValues,
              ]),
            }
          }
          return tp
        }),
      }
    case 'SET_ACTIVE_TELEGRAF_PLUGIN':
      return {
        ...state,
        telegrafPlugins: state.telegrafPlugins.map(tp => {
          if (tp.name === action.payload.telegrafPlugin) {
            return {...tp, active: true}
          }
          return {...tp, active: false}
        }),
      }
    case 'SET_PLUGIN_CONFIGURATION_STATE':
      return {
        ...state,
        telegrafPlugins: state.telegrafPlugins.map(tp => {
          const name = _.get(tp, 'name')
          if (name === action.payload.telegrafPlugin) {
            const configFields = getConfigFields(name)
            if (!configFields) {
              return {...tp, configured: ConfigurationState.Configured}
            }

            const {config} = getDeep<Plugin>(
              tp,
              'plugin',
              createNewPlugin(name)
            )

            let isValidConfig = true

            Object.entries(configFields).forEach(
              ([fieldName, {type: fieldType, isRequired}]) => {
                if (isRequired) {
                  const fieldValue = config[fieldName]

                  const isValidUri =
                    fieldType === ConfigFieldType.Uri &&
                    validateURI(fieldValue as string)
                  const isValidString =
                    fieldType === ConfigFieldType.String &&
                    (fieldValue as string) !== ''
                  const isValidArray =
                    (fieldType === ConfigFieldType.StringArray ||
                      fieldType === ConfigFieldType.UriArray) &&
                    (fieldValue as string[]).length

                  if (!isValidUri && !isValidString && !isValidArray) {
                    isValidConfig = false
                  }
                }
              }
            )

            if (!isValidConfig || _.isEmpty(config)) {
              return {...tp, configured: ConfigurationState.Unconfigured}
            } else {
              return {...tp, configured: ConfigurationState.Configured}
            }
          }
          return {...tp}
        }),
      }
    case 'SET_LINE_PROTOCOL_BODY':
      return {
        ...state,
        lineProtocolBody: action.payload.lineProtocolBody,
      }
    case 'SET_ACTIVE_LP_TAB':
      return {
        ...state,
        activeLPTab: action.payload.activeLPTab,
      }
    case 'SET_LP_STATUS':
      return {
        ...state,
        lpStatus: action.payload.lpStatus,
      }
    case 'SET_PRECISION':
      return {
        ...state,
        precision: action.payload.precision,
      }
    default:
      return state
  }
}
