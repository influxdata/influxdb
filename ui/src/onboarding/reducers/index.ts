// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {Action} from 'src/onboarding/actions'
import {SetupParams} from 'src/onboarding/apis'
import {DataSource} from 'src/types/v2/dataSources'

export interface OnboardingState {
  currentStepIndex: number
  stepStatuses: StepStatus[]
  setupParams: SetupParams
  dataSources: DataSource[]
}

const INITIAL_STATE: OnboardingState = {
  currentStepIndex: 0,
  stepStatuses: [
    StepStatus.Incomplete,
    StepStatus.Incomplete,
    StepStatus.Incomplete,
    StepStatus.Incomplete,
    StepStatus.Incomplete,
  ],
  dataSources: [],
  setupParams: null,
}

export default (state = INITIAL_STATE, action: Action): OnboardingState => {
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
    case 'ADD_DATA_SOURCE':
      return {
        ...state,
        dataSources: [...state.dataSources, action.payload.dataSource],
      }
    case 'REMOVE_DATA_SOURCE':
      return {
        ...state,
        dataSources: state.dataSources.filter(
          ds => ds.name !== action.payload.dataSource
        ),
      }
    default:
      return state
  }
}
