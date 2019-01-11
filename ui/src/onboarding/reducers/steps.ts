// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {Action} from 'src/onboarding/actions/steps'
import {SetupParams} from 'src/onboarding/apis'

export interface OnboardingStepsState {
  stepStatuses: StepStatus[]
  setupParams: SetupParams
}

const INITIAL_STATE: OnboardingStepsState = {
  stepStatuses: new Array(6).fill(StepStatus.Incomplete),
  setupParams: null,
}

export default (
  state = INITIAL_STATE,
  action: Action
): OnboardingStepsState => {
  switch (action.type) {
    case 'SET_SETUP_PARAMS':
      return {...state, setupParams: action.payload.setupParams}
    case 'SET_STEP_STATUS':
      const stepStatuses = [...state.stepStatuses]
      stepStatuses[action.payload.index] = action.payload.status
      return {...state, stepStatuses}
    default:
      return state
  }
}
