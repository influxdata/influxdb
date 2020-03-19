// Libraries
import {filter} from 'lodash'
// Types
import {AppState, Bucket} from 'src/types'

export const getDataLoaders = (state: AppState) => {
  return state.dataLoading.dataLoaders
}

export const getSteps = (state: AppState) => {
  return state.dataLoading.steps
}

export const getBucketByName = (
  state: AppState,
  bucketName: string
): Bucket => {
  const buckets = state.resources.buckets.byID
  const [bucket] = filter(buckets, b => {
    return b.name === bucketName
  })
  return bucket
}
