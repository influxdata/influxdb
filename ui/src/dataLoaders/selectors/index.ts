import {AppState} from 'src/types'

export const getDataLoaders = (state: AppState) => {
  return state.dataLoading.dataLoaders
}

export const getSteps = (state: AppState) => {
  return state.dataLoading.steps
}
