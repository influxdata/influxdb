// Libraries
import {combineReducers} from 'redux'

// Reducers
import dataLoadersReducer from 'src/onboarding/reducers/dataLoaders'
import {DataLoadersState} from 'src/types/v2/dataLoaders'
import stepsReducer, {OnboardingStepsState} from 'src/onboarding/reducers/steps'

export interface OnboardingState {
  steps: OnboardingStepsState
  dataLoaders: DataLoadersState
}

export default combineReducers<OnboardingState>({
  steps: stepsReducer,
  dataLoaders: dataLoadersReducer,
})
