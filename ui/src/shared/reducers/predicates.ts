// Actions
import {Action} from 'src/shared/actions/predicates'

// Types
import {PredicatesState, RemoteDataState} from 'src/types'

export const initialState: PredicatesState = {
  bucketName: '',
  deletionStatus: RemoteDataState.NotStarted,
  files: [],
  filters: [],
  isSerious: false,
  keys: [],
  previewStatus: RemoteDataState.NotStarted,
  timeRange: null,
  values: [],
}

export const predicatesReducer = (
  state: PredicatesState = initialState,
  action: Action
): PredicatesState => {
  switch (action.type) {
    case 'RESET_FILTERS':
      return {...state, filters: []}

    case 'SET_IS_SERIOUS':
      return {...state, isSerious: action.payload.isSerious}

    case 'SET_BUCKET_NAME':
      return {...state, bucketName: action.payload.bucketName}

    case 'SET_DELETE_TIME_RANGE':
      return {...state, timeRange: action.payload.timeRange}

    case 'SET_FILTER':
      if (action.payload.index >= state.filters.length) {
        return {...state, filters: [...state.filters, action.payload.filter]}
      }

      return {
        ...state,
        filters: state.filters.map((filter, i) =>
          i === action.payload.index ? action.payload.filter : filter
        ),
      }

    case 'DELETE_FILTER':
      return {
        ...state,
        filters: state.filters.filter((_, i) => i !== action.payload.index),
      }

    case 'SET_DELETION_STATUS':
      return {...state, deletionStatus: action.payload.deletionStatus}

    case 'SET_FILES':
      return {
        ...state,
        files: action.payload.files,
        previewStatus: RemoteDataState.Done,
      }

    case 'SET_KEYS_BY_BUCKET':
      return {...state, keys: action.payload.keys}

    case 'SET_PREVIEW_STATUS':
      return {...state, previewStatus: action.payload.previewStatus}

    case 'SET_VALUES_BY_KEY':
      return {...state, values: action.payload.values}

    case 'SET_PREDICATE_DEFAULT':
      return {...initialState}

    default:
      return state
  }
}
