import {Dispatch} from 'redux-thunk'

// API
import {client} from 'src/utils/api'

// Types
import {RemoteDataState, AppState, Bucket} from 'src/types'

// Utils
import {isLimitError} from 'src/cloud/utils/limits'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {checkBucketLimits} from 'src/cloud/actions/limits'

// Constants
import {
  getBucketsFailed,
  resourceLimitReached,
} from 'src/shared/copy/notifications'
import {
  bucketCreateFailed,
  bucketUpdateFailed,
  bucketDeleteFailed,
  bucketUpdateSuccess,
  bucketRenameSuccess,
  bucketRenameFailed,
} from 'src/shared/copy/v2/notifications'

export type Action = SetBuckets | AddBucket | EditBucket | RemoveBucket

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

    const buckets = await client.buckets.getAll(org.id)

    dispatch(setBuckets(RemoteDataState.Done, buckets))
  } catch (e) {
    console.error(e)
    dispatch(setBuckets(RemoteDataState.Error))
    dispatch(notify(getBucketsFailed()))
  }
}

export const createBucket = (bucket: Bucket) => async (
  dispatch: Dispatch<Action>,
  getState: () => AppState
) => {
  try {
    const {
      orgs: {org},
    } = getState()

    const createdBucket = await client.buckets.create({
      ...bucket,
      organizationID: org.id,
    })

    dispatch(addBucket(createdBucket))
    dispatch(checkBucketLimits())
  } catch (error) {
    console.error(error)
    if (isLimitError(error)) {
      dispatch(notify(resourceLimitReached('buckets')))
    } else {
      dispatch(notify(bucketCreateFailed()))
    }
  }
}

export const updateBucket = (updatedBucket: Bucket) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const bucket = await client.buckets.update(updatedBucket.id, updatedBucket)

    dispatch(editBucket(bucket))
    dispatch(notify(bucketUpdateSuccess(updatedBucket.name)))
  } catch (e) {
    console.error(e)
    dispatch(notify(bucketUpdateFailed(updatedBucket.name)))
  }
}

export const renameBucket = (
  originalName: string,
  updatedBucket: Bucket
) => async (dispatch: Dispatch<Action>) => {
  try {
    const bucket = await client.buckets.update(updatedBucket.id, updatedBucket)

    dispatch(editBucket(bucket))
    dispatch(notify(bucketRenameSuccess(updatedBucket.name)))
  } catch (e) {
    console.error(e)
    dispatch(notify(bucketRenameFailed(originalName)))
  }
}

export const deleteBucket = (id: string, name: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    await client.buckets.delete(id)

    dispatch(removeBucket(id))
    dispatch(checkBucketLimits())
  } catch (e) {
    console.error(e)
    dispatch(notify(bucketDeleteFailed(name)))
  }
}
