// Libraries
import moment from 'moment'

// Actions
import {Action} from 'src/shared/actions/predicates'

// Types
import {PredicatesState, RemoteDataState} from 'src/types'

export const recently = Date.parse(moment().format('YYYY-MM-DD HH:00:00'))
export const HOUR_MS = 1000 * 60 * 60

export const initialState: PredicatesState = {
  bucketName: '',
  timeRange: [recently - HOUR_MS, recently],
  filters: [],
  isSerious: false,
  deletionStatus: RemoteDataState.NotStarted,
}

export const predicatesReducer = (
  state: PredicatesState = initialState,
  action: Action
): PredicatesState => {
  switch (action.type) {
    case 'RESET_FILTERS':
      return {...state, filters: []}

    case 'SET_IS_SERIOUS':
      return {...state, isSerious: action.isSerious}

    case 'SET_BUCKET_NAME':
      return {...state, bucketName: action.bucketName}

    case 'SET_DELETE_TIME_RANGE':
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

    case 'SET_PREDICATE_DEFAULT':
      return {
        bucketName: '',
        timeRange: [recently - HOUR_MS, recently],
        filters: [],
        isSerious: false,
        deletionStatus: RemoteDataState.NotStarted,
      }

    default:
      return state
  }
}
