import {
  Actions,
  SET_FEATURE_FLAGS,
  RESET_FEATURE_FLAGS,
  CLEAR_FEATURE_FLAG_OVERRIDES,
  SET_FEATURE_FLAG_OVERRIDE,
} from 'src/shared/actions/flags'
import {RemoteDataState} from 'src/types'

export interface FlagMap {
  [key: string]: string | boolean
}

export interface FlagState {
  status: RemoteDataState
  original: FlagMap
  override: FlagMap
}

const defaultState: FlagState = {
  status: RemoteDataState.NotStarted,
  original: {},
  override: {},
}

export default (state = defaultState, action: Actions): FlagState => {
  switch (action.type) {
    case SET_FEATURE_FLAGS:
      // just setting the loading state
      if (!action.payload.flags) {
        const newState = {
          ...state,
          status: action.payload.status,
        }

        if (!state.hasOwnProperty('original')) {
          newState.original = defaultState.original
        }

        return newState
      }
      return {
        ...state,
        status: action.payload.status,
        original: action.payload.flags,
      }
    case RESET_FEATURE_FLAGS:
      return {
        ...defaultState,
      }
    case CLEAR_FEATURE_FLAG_OVERRIDES:
      return {
        ...state,
        override: {},
      }
    case SET_FEATURE_FLAG_OVERRIDE:
      const override = {
        ...(state.override || {}),
        ...action.payload,
      }
      return {
        ...state,
        override,
      }
    default:
      return state
  }
}
