//  Utils
import {getInitialDataSources} from 'src/onboarding/utils/dataLoaders'

// Types
import {Action} from 'src/onboarding/actions/dataLoaders'
import {DataSource, DataLoaderType} from 'src/types/v2/dataLoaders'

export interface DataLoadersState {
  dataSources: DataSource[]
  type: DataLoaderType
}

export const INITIAL_STATE: DataLoadersState = {
  dataSources: [],
  type: DataLoaderType.Empty,
}

export default (state = INITIAL_STATE, action: Action): DataLoadersState => {
  switch (action.type) {
    case 'SET_DATA_LOADERS_TYPE':
      return {
        ...state,
        type: action.payload.type,
        dataSources: getInitialDataSources(action.payload.type),
      }
    case 'ADD_DATA_SOURCE':
      return {
        ...state,
        dataSources: [...state.dataSources, action.payload.dataSource],
      }
    case 'REMOVE_DATA_SOURCE':
      return {
        ...state,
        dataSources: state.dataSources.filter(
          ds => ds.name !== action.payload.dataSource
        ),
      }
    case 'SET_ACTIVE_DATA_SOURCE':
      return {
        ...state,
        dataSources: state.dataSources.map(ds => {
          if (ds.name === action.payload.dataSource) {
            return {...ds, active: true}
          }
          return {...ds, active: false}
        }),
      }
    default:
      return state
  }
}
