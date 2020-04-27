import {
  Actions,
  SET_FEATURE_FLAGS,
  CLEAR_FEATURE_FLAG_OVERRIDES,
  SET_FEATURE_FLAG_OVERRIDE,
} from 'src/shared/actions/flags'

export interface FlagMap {
  [key: string]: string | boolean
}

export interface FlagState {
  original: FlagMap
  override: FlagMap
}

const defaultState: FlagState = {
  original: {},
  override: {},
}

export default (state = defaultState, action: Actions): FlagState => {
  switch (action.type) {
    case SET_FEATURE_FLAGS:
      return {
        ...state,
        original: action.payload,
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
