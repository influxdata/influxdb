// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {Action} from 'src/onboarding/actions'
import {SetupParams} from 'src/onboarding/apis'

export interface OnboardingState {
  stepStatuses: StepStatus[]
  setupParams: SetupParams
  orgID: string
  bucketID: string
}

const INITIAL_STATE: OnboardingState = {
  stepStatuses: new Array(3).fill(StepStatus.Incomplete),
  setupParams: null,
  orgID: '',
  bucketID: '',
}

export default (state = INITIAL_STATE, action: Action): OnboardingState => {
  switch (action.type) {
    case 'SET_SETUP_PARAMS':
      return {...state, setupParams: action.payload.setupParams}
    case 'SET_STEP_STATUS':
      const stepStatuses = [...state.stepStatuses]
      stepStatuses[action.payload.index] = action.payload.status
      return {...state, stepStatuses}
    case 'SET_ORG_ID':
      return {...state, orgID: action.payload.orgID}
    case 'SET_BUCKET_ID':
      return {...state, bucketID: action.payload.bucketID}
    default:
      return state
  }
}
