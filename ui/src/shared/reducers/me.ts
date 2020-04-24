import {Actions, SET_ME} from 'src/shared/actions/me'

export interface MeLinks {
  self: string
  log: string
}

export interface MeState {
  id: string
  name: string
  links: MeLinks
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
    default:
      return state
  }
}
