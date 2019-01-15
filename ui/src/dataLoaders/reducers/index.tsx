// Libraries
import {combineReducers} from 'redux'

// Reducers
import dataLoadersReducer from 'src/onboarding/reducers/dataLoaders'
import {DataLoadersState} from 'src/types/v2/dataLoaders'
import stepsReducer, {
  DataLoadersStepsState,
} from 'src/onboarding/reducers/steps'

export interface DataLoadingState {
  steps: DataLoadersStepsState
  dataLoaders: DataLoadersState
}

export default combineReducers<DataLoadingState>({
  steps: stepsReducer,
  dataLoaders: dataLoadersReducer,
})
