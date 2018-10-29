import {Actions, ActionTypes} from 'src/shared/actions/v2/me'

export interface MeState {
  id: string
  name: string
}

const defaultState = {
  id: '',
  name: '',
}

export default (state = defaultState, action: Actions): MeState => {
  switch (action.type) {
    case ActionTypes.SetMe:
      return action.payload.me
    default:
      return state
  }
}
