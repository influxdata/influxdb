// Libraries
import {combineReducers} from 'redux'

// Reducers
import dataSourcesReducer, {
  DataSourcesState,
} from 'src/onboarding/reducers/dataSources'
import stepsReducer, {OnboardingStepsState} from 'src/onboarding/reducers/steps'

export interface OnboardingState {
  steps: OnboardingStepsState
  dataSources: DataSourcesState
}

export default combineReducers<OnboardingState>({
  steps: stepsReducer,
  dataSources: dataSourcesReducer,
})
