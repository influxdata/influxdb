import {Action, ActionTypes} from 'src/dashboards/actions/v2/views'

type State = string

const initialState = ''

export default (state: State = initialState, action: Action) => {
  switch (action.type) {
    case ActionTypes.SetActiveView: {
      const {activeViewID} = action.payload
      return activeViewID
    }
  }

  return state
}
