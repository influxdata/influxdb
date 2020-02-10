// Libraries
import {normalize} from 'normalizr'
import {Dispatch} from 'react'

// API
import * as api from 'src/client'

// Schemas
import {bucketSchema, arrayOfBuckets} from 'src/schemas'

// Types
import {RemoteDataState, GetState, Bucket, BucketEntities} from 'src/types'

// Utils
import {getErrorMessage} from 'src/utils/api'
import {getOrg} from 'src/organizations/selectors'

// Actions
import {
  editBucket,
  addBucket,
  setBuckets,
  removeBucket,
  Action as BucketAction,
} from 'src/buckets/actions/creators'
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'
import {checkBucketLimits} from 'src/cloud/actions/limits'

// Constants
import {
  getBucketsFailed,
  bucketCreateFailed,
  bucketUpdateFailed,
  bucketDeleteFailed,
  bucketUpdateSuccess,
  bucketRenameSuccess,
  bucketRenameFailed,
} from 'src/shared/copy/notifications'

type Action = BucketAction | NotifyAction

export const getBuckets = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    dispatch(setBuckets(RemoteDataState.Loading))
    const org = getOrg(getState())

    const resp = await api.getBuckets({query: {orgID: org.id}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const buckets = normalize<Bucket, BucketEntities, string[]>(
      resp.data.buckets,
      arrayOfBuckets
    )

    dispatch(setBuckets(RemoteDataState.Done, buckets))
  } catch (e) {
    console.error(e)
    dispatch(setBuckets(RemoteDataState.Error))
    dispatch(notify(getBucketsFailed()))
  }
}

export const createBucket = (bucket: Bucket) => async (
  dispatch: Dispatch<Action | ReturnType<typeof checkBucketLimits>>,
  getState: GetState
) => {
  try {
    const org = getOrg(getState())

    const resp = await api.postBucket({data: {...bucket, orgID: org.id}})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const newBucket = normalize<Bucket, BucketEntities, string>(
      resp.data,
      bucketSchema
    )

    dispatch(addBucket(newBucket))
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

    const newBucket = normalize<Bucket, BucketEntities, string>(
      resp.data,
      bucketSchema
    )

    dispatch(editBucket(newBucket))
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

    const newBucket = normalize<Bucket, BucketEntities, string>(
      resp.data,
      bucketSchema
    )

    dispatch(editBucket(newBucket))
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
