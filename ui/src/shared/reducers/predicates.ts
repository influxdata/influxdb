// Libraries
import moment from 'moment'

// Actions
import {Action} from 'src/shared/actions/predicates'

// Types
import {RemoteDataState, Filter} from 'src/types'

export interface PredicatesState {
  bucketName: string
  timeRange: [number, number]
  filters: Filter[]
  isSerious: boolean
  deletionStatus: RemoteDataState
}

const recently = Date.parse(moment().format('YYYY-MM-DD HH:00:00'))
const HOUR_MS = 1000 * 60 * 60

const initialState: PredicatesState = {
  bucketName: '',
  timeRange: [recently - HOUR_MS, recently],
  filters: [],
  isSerious: false,
  deletionStatus: RemoteDataState.NotStarted,
}

const predicatesReducer = (
  state: PredicatesState = initialState,
  action: Action
): PredicatesState => {
  switch (action.type) {
    case 'SET_IS_SERIOUS':
      return {...state, isSerious: action.isSerious}

    case 'SET_BUCKET_NAME':
      return {...state, bucketName: action.bucketName}

    case 'SET_TIME_RANGE':
      return {...state, timeRange: action.timeRange}

    case 'SET_FILTER':
      if (action.index >= state.filters.length) {
        return {...state, filters: [...state.filters, action.filter]}
      }

      return {
        ...state,
        filters: state.filters.map((filter, i) =>
          i === action.index ? action.filter : filter
        ),
      }

    case 'DELETE_FILTER':
      return {
        ...state,
        filters: state.filters.filter((_, i) => i !== action.index),
      }

    case 'SET_DELETION_STATUS':
      return {...state, deletionStatus: action.deletionStatus}

    default:
      return state
  }
}

export default predicatesReducer
