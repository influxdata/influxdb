// Libraries
import {combineReducers} from 'redux'

// Reducers
import dataLoadersReducer, {
  DataLoadersState,
} from 'src/onboarding/reducers/dataLoaders'
import stepsReducer, {OnboardingStepsState} from 'src/onboarding/reducers/steps'

export interface OnboardingState {
  steps: OnboardingStepsState
  dataLoaders: DataLoadersState
}

export default combineReducers<OnboardingState>({
  steps: stepsReducer,
  dataLoaders: dataLoadersReducer,
})
