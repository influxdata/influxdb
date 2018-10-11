import {Action, ActionTypes} from 'src/tasks/actions/v2'

export interface State {
  newScript: string
}

const defaultState: State = {
  newScript: '',
}

export default (state: State = defaultState, action: Action): State => {
  switch (action.type) {
    case ActionTypes.SetNewScript:
      return {...state, newScript: action.payload.script}
    default:
      return state
  }
}
