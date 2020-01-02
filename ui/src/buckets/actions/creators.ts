// Types
import {RemoteDataState, Bucket} from 'src/types'
import {Action as NotifyAction} from 'src/shared/actions/notifications'

export type Action =
  | SetBuckets
  | AddBucket
  | EditBucket
  | RemoveBucket
  | NotifyAction

interface SetBuckets {
  type: 'SET_BUCKETS'
  payload: {
    status: RemoteDataState
    list: Bucket[]
  }
}

export const setBuckets = (
  status: RemoteDataState,
  list?: Bucket[]
): SetBuckets => ({
  type: 'SET_BUCKETS',
  payload: {status, list},
})

interface AddBucket {
  type: 'ADD_BUCKET'
  payload: {
    bucket: Bucket
  }
}

export const addBucket = (bucket: Bucket): AddBucket => ({
  type: 'ADD_BUCKET',
  payload: {bucket},
})

interface EditBucket {
  type: 'EDIT_BUCKET'
  payload: {
    bucket: Bucket
  }
}

export const editBucket = (bucket: Bucket): EditBucket => ({
  type: 'EDIT_BUCKET',
  payload: {bucket},
})

interface RemoveBucket {
  type: 'REMOVE_BUCKET'
  payload: {id: string}
}

export const removeBucket = (id: string): RemoveBucket => ({
  type: 'REMOVE_BUCKET',
  payload: {id},
})
