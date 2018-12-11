// Libraries
import _ from 'lodash'

// Utils
import {
  createNewPlugin,
  updateConfigFields,
} from 'src/onboarding/utils/pluginConfigs'

// Types
import {Action} from 'src/onboarding/actions/dataLoaders'
import {
  DataLoaderType,
  LineProtocolTab,
  DataLoadersState,
} from 'src/types/v2/dataLoaders'
import {RemoteDataState} from 'src/types'
import {WritePrecision} from 'src/api'

export const INITIAL_STATE: DataLoadersState = {
  telegrafPlugins: [],
  type: DataLoaderType.Empty,
  lineProtocolBody: '',
  activeLPTab: LineProtocolTab.UploadFile,
  lpStatus: RemoteDataState.NotStarted,
  precision: WritePrecision.Ms,
  telegrafConfigID: null,
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
    case 'ADD_TELEGRAF_PLUGIN':
      return {
        ...state,
        telegrafPlugins: [
          ...state.telegrafPlugins,
          action.payload.telegrafPlugin,
        ],
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
    case 'REMOVE_TELEGRAF_PLUGIN':
      return {
        ...state,
        telegrafPlugins: state.telegrafPlugins.filter(
          tp => tp.name !== action.payload.telegrafPlugin
        ),
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
