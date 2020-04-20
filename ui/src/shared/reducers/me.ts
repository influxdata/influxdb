import {
  Actions,
  SET_ME,
  CLEAR_FEATURE_FLAG_OVERRIDES,
  SET_FEATURE_FLAG_OVERRIDE,
} from 'src/shared/actions/me'

export interface MeLinks {
  self: string
  log: string
}

export interface MeFlags {
  [key: string]: string | boolean
}

export interface MeState {
  id: string
  name: string
  links: MeLinks
  flags?: MeFlags
  flagOverrides?: MeFlags
}

const defaultState: MeState = {
  id: '',
  name: '',
  links: {
    self: '',
    log: '',
  },
}

export default (state = defaultState, action: Actions): MeState => {
  switch (action.type) {
    case SET_ME:
      return {
        ...state,
        ...action.payload.me,
      }
    case CLEAR_FEATURE_FLAG_OVERRIDES:
      return {
        ...state,
        flagOverrides: {},
      }
    case SET_FEATURE_FLAG_OVERRIDE:
      const flagOverrides = {
        ...(state.flagOverrides || {}),
        ...action.payload,
      }
      return {
        ...state,
        flagOverrides,
      }
    default:
      return state
  }
}
