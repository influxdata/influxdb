// Types
import {Action} from 'src/onboarding/actions/dataLoaders'
import {
  TelegrafPlugin,
  DataLoaderType,
  LineProtocolTab,
} from 'src/types/v2/dataLoaders'

export interface DataLoadersState {
  telegrafPlugins: TelegrafPlugin[]
  type: DataLoaderType
  lineProtocolText: string
  activeLPTab: LineProtocolTab
}

export const INITIAL_STATE: DataLoadersState = {
  telegrafPlugins: [],
  type: DataLoaderType.Empty,
  lineProtocolText: '',
  activeLPTab: LineProtocolTab.UploadFile,
}

export default (state = INITIAL_STATE, action: Action): DataLoadersState => {
  switch (action.type) {
    case 'SET_DATA_LOADERS_TYPE':
      return {
        ...state,
        type: action.payload.type,
      }
    case 'ADD_TELEGRAF_PLUGIN':
      return {
        ...state,
        telegrafPlugins: [
          ...state.telegrafPlugins,
          action.payload.telegrafPlugin,
        ],
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
    case 'SET_LINE_PROTOCOL_TEXT':
      return {
        ...state,
        lineProtocolText: action.payload.lineProtocolText,
      }
    case 'SET_ACTIVE_LP_TAB':
      return {
        ...state,
        activeLPTab: action.payload.activeLPTab,
      }
    default:
      return state
  }
}
