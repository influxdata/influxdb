// Libraries
import {combineReducers} from 'redux'

// Reducers
import dataLoadersReducer from 'src/dataLoaders/reducers/dataLoaders'
import {DataLoadersState} from 'src/types/dataLoaders'
import stepsReducer, {
  DataLoadersStepsState,
} from 'src/dataLoaders/reducers/steps'

export interface DataLoadingState {
  steps: DataLoadersStepsState
  dataLoaders: DataLoadersState
}

export default combineReducers<DataLoadingState>({
  steps: stepsReducer,
  dataLoaders: dataLoadersReducer,
})
