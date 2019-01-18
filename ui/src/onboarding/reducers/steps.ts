// Types
import {Action} from 'src/onboarding/actions/steps'
import {Substep} from 'src/types/v2/dataLoaders'

export interface DataLoadersStepsState {
  currentStep: number
  substep?: Substep
  orgID: string
  bucketID: string
  org: string
  bucket: string
}

const INITIAL_STATE: DataLoadersStepsState = {
  org: '',
  bucket: '',
  orgID: '',
  bucketID: '',
  currentStep: 0,
}

export default (
  state = INITIAL_STATE,
  action: Action
): DataLoadersStepsState => {
  switch (action.type) {
    case 'CLEAR_STEPS':
      return {...INITIAL_STATE}
    case 'INCREMENT_CURRENT_STEP_INDEX':
      return {...state, currentStep: state.currentStep + 1}
    case 'DECREMENT_CURRENT_STEP_INDEX':
      return {...state, currentStep: state.currentStep - 1}
    case 'SET_CURRENT_STEP_INDEX':
      return {...state, currentStep: action.payload.index}
    case 'SET_SUBSTEP_INDEX':
      return {
        ...state,
        currentStep: action.payload.stepIndex,
        substep: action.payload.substep,
      }
    case 'SET_BUCKET_INFO':
      return {...state, ...action.payload}
    case 'SET_BUCKET_ID':
      return {...state, bucketID: action.payload.bucketID}
    default:
      return state
  }
}
