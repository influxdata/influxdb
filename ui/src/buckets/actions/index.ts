import {Dispatch} from 'react'

// API
import * as api from 'src/client'

// Types
import {RemoteDataState, AppState, Bucket} from 'src/types'

// Utils
import {getErrorMessage} from 'src/utils/api'

// Actions
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'
import {checkBucketLimits} from 'src/cloud/actions/limits'

// Constants
import {getBucketsFailed} from 'src/shared/copy/notifications'
import {
  bucketCreateFailed,
  bucketUpdateFailed,
  bucketDeleteFailed,
  bucketUpdateSuccess,
  bucketRenameSuccess,
  bucketRenameFailed,
} from 'src/shared/copy/notifications'

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

export const getBuckets = () => async (
  dispatch: Dispatch<Action>,
  getState: () => AppState
) => {
  try {
    dispatch(setBuckets(RemoteDataState.Loading))
    const {
      orgs: {org},
    } = getState()

    const resp = await api.getBuckets({query: {orgID: org.id}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(setBuckets(RemoteDataState.Done, resp.data.buckets))
  } catch (e) {
    console.error(e)
    dispatch(setBuckets(RemoteDataState.Error))
    dispatch(notify(getBucketsFailed()))
  }
}

export const createBucket = (bucket: Bucket) => async (
  dispatch: Dispatch<Action | ReturnType<typeof checkBucketLimits>>,
  getState: () => AppState
) => {
  try {
    const {
      orgs: {org},
    } = getState()

    const resp = await api.postBucket({data: {...bucket, orgID: org.id}})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    dispatch(addBucket(resp.data))
    dispatch(checkBucketLimits())
  } catch (error) {
    console.error(error)
    const message = getErrorMessage(error)
    dispatch(notify(bucketCreateFailed(message)))
  }
}

export const updateBucket = (updatedBucket: Bucket) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const resp = await api.patchBucket({
      bucketID: updatedBucket.id,
      data: updatedBucket,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(editBucket(resp.data))
    dispatch(notify(bucketUpdateSuccess(updatedBucket.name)))
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(bucketUpdateFailed(message)))
  }
}

export const renameBucket = (
  originalName: string,
  updatedBucket: Bucket
) => async (dispatch: Dispatch<Action>) => {
  try {
    const resp = await api.patchBucket({
      bucketID: updatedBucket.id,
      data: updatedBucket,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(editBucket(resp.data))
    dispatch(notify(bucketRenameSuccess(updatedBucket.name)))
  } catch (e) {
    console.error(e)
    dispatch(notify(bucketRenameFailed(originalName)))
  }
}

export const deleteBucket = (id: string, name: string) => async (
  dispatch: Dispatch<Action | ReturnType<typeof checkBucketLimits>>
) => {
  try {
    const resp = await api.deleteBucket({bucketID: id})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeBucket(id))
    dispatch(checkBucketLimits())
  } catch (e) {
    console.error(e)
    dispatch(notify(bucketDeleteFailed(name)))
  }
}
