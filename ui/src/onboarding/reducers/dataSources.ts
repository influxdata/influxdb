// Types
import {Action} from 'src/onboarding/actions/dataSources'
import {DataSource} from 'src/types/v2/dataSources'

export type DataSourcesState = DataSource[]

const INITIAL_STATE: DataSourcesState = []

export default (state = INITIAL_STATE, action: Action): DataSourcesState => {
  switch (action.type) {
    case 'ADD_DATA_SOURCE':
      return [...state, action.payload.dataSource]
    case 'REMOVE_DATA_SOURCE':
      return state.filter(ds => ds.name !== action.payload.dataSource)
    case 'SET_DATA_SOURCES':
      return action.payload.dataSources
    case 'SET_ACTIVE_DATA_SOURCE':
      return state.map(ds => {
        if (ds.name === action.payload.dataSource) {
          return {...ds, active: true}
        }
        return {...ds, active: false}
      })
    default:
      return state
  }
}
