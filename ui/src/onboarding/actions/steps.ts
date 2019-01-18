// Types
import {Substep} from 'src/types/v2/dataLoaders'

export type Action =
  | SetBucketInfo
  | SetBucketID
  | IncrementCurrentStepIndex
  | DecrementCurrentStepIndex
  | SetCurrentStepIndex
  | SetSubstepIndex
  | ClearSteps

interface ClearSteps {
  type: 'CLEAR_STEPS'
}

export const clearSteps = (): ClearSteps => ({type: 'CLEAR_STEPS'})

interface IncrementCurrentStepIndex {
  type: 'INCREMENT_CURRENT_STEP_INDEX'
}

export const incrementCurrentStepIndex = (): IncrementCurrentStepIndex => ({
  type: 'INCREMENT_CURRENT_STEP_INDEX',
})

interface DecrementCurrentStepIndex {
  type: 'DECREMENT_CURRENT_STEP_INDEX'
}

export const decrementCurrentStepIndex = (): DecrementCurrentStepIndex => ({
  type: 'DECREMENT_CURRENT_STEP_INDEX',
})

interface SetCurrentStepIndex {
  type: 'SET_CURRENT_STEP_INDEX'
  payload: {index: number}
}

export const setCurrentStepIndex = (index: number): SetCurrentStepIndex => ({
  type: 'SET_CURRENT_STEP_INDEX',
  payload: {index},
})

interface SetSubstepIndex {
  type: 'SET_SUBSTEP_INDEX'
  payload: {stepIndex: number; substep: Substep}
}

export const setSubstepIndex = (
  stepIndex: number,
  substep: Substep
): SetSubstepIndex => ({
  type: 'SET_SUBSTEP_INDEX',
  payload: {stepIndex, substep},
})

interface SetBucketInfo {
  type: 'SET_BUCKET_INFO'
  payload: {
    org: string
    orgID: string
    bucket: string
    bucketID: string
  }
}

export const setBucketInfo = (
  org: string,
  orgID: string,
  bucket: string,
  bucketID: string
): SetBucketInfo => ({
  type: 'SET_BUCKET_INFO',
  payload: {org, orgID, bucket, bucketID},
})

interface SetBucketID {
  type: 'SET_BUCKET_ID'
  payload: {bucketID: string}
}

export const setBucketID = (bucketID: string): SetBucketID => ({
  type: 'SET_BUCKET_ID',
  payload: {bucketID},
})
