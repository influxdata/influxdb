import {Actions, ActionTypes} from 'src/shared/actions/v2/me'

export interface MeLinks {
  self: string
  log: string
}

export interface MeState {
  id: string
  name: string
  links: MeLinks
}

const defaultState = {
  id: '',
  name: '',
  links: {
    self: '',
    log: '',
  },
}

export default (state = defaultState, action: Actions): MeState => {
  switch (action.type) {
    case ActionTypes.SetMe:
      return action.payload.me
    default:
      return state
  }
}
