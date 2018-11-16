// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {Action} from 'src/onboarding/actions/steps'
import {SetupParams} from 'src/onboarding/apis'

export interface OnboardingStepsState {
  currentStepIndex: number
  stepStatuses: StepStatus[]
  setupParams: SetupParams
}

const INITIAL_STATE: OnboardingStepsState = {
  currentStepIndex: 0,
  stepStatuses: new Array(5).fill(StepStatus.Incomplete),
  setupParams: null,
}

export default (
  state = INITIAL_STATE,
  action: Action
): OnboardingStepsState => {
  switch (action.type) {
    case 'SET_SETUP_PARAMS':
      return {...state, setupParams: action.payload.setupParams}
    case 'INCREMENT_CURRENT_STEP_INDEX':
      return {...state, currentStepIndex: state.currentStepIndex + 1}
    case 'DECREMENT_CURRENT_STEP_INDEX':
      return {...state, currentStepIndex: state.currentStepIndex - 1}
    case 'SET_CURRENT_STEP_INDEX':
      return {...state, currentStepIndex: action.payload.index}
    case 'SET_STEP_STATUS':
      const stepStatuses = [...state.stepStatuses]
      stepStatuses[action.payload.index] = action.payload.status
      return {...state, stepStatuses}
    default:
      return state
  }
}
