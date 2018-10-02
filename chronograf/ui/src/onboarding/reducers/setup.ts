import {Action, ActionTypes} from 'src/onboarding/actions'

type State = boolean
const initialisSetupComplete: State = false

const setupReducer = (
  state: State = initialisSetupComplete,
  action: Action
): State => {
  switch (action.type) {
    case ActionTypes.CompleteSetup: {
      const {isSetupComplete} = action.payload
      return isSetupComplete
    }
  }

  return state
}

export default setupReducer
