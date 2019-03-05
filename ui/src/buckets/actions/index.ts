// API
import {client} from 'src/utils/api'

// Types
import {RemoteDataState} from 'src/types'
import {Bucket} from '@influxdata/influx'
import {Dispatch} from 'redux-thunk'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {getBucketsFailed} from 'src/shared/copy/notifications'

import {
  //   bucketDeleteSuccess,
  //   bucketDeleteFailed,
  bucketCreateFailed,
  //   bucketCreateSuccess,
  bucketUpdateFailed,
  //   bucketUpdateSuccess,
} from 'src/shared/copy/v2/notifications'

export type Action = SetBuckets | AddBucket | EditBucket

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
  payload: {bucket}
}

export const editLabel = (bucket: Bucket): EditBucket => ({
  type: 'EDIT_BUCKET',
  payload: {bucket},
})

export const getBuckets = () => async (dispatch: Dispatch<Action>) => {
  try {
    dispatch(setBuckets(RemoteDataState.Loading))

    const buckets = (await client.buckets.getAllByOrg('')) as Bucket[]

    dispatch(setBuckets(RemoteDataState.Done, buckets))
  } catch (e) {
    console.log(e)
    dispatch(setBuckets(RemoteDataState.Error))
    dispatch(notify(getBucketsFailed()))
  }
}

export const createBucket = (bucket: Bucket) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const createdBucket = await client.buckets.create(bucket)
    dispatch(addBucket(createdBucket))
  } catch (e) {
    console.log(e)
    dispatch(notify(bucketCreateFailed()))
    throw e
  }
}

export const updateBucket = (bucket: Bucket) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const label = await client.buckets.update(bucket.id, bucket)

    dispatch(editLabel(label))
  } catch (e) {
    console.log(e)
    dispatch(notify(bucketUpdateFailed(bucket.name)))
  }
}

//
// interface RemoveLabel {
//   type: 'REMOVE_LABEL'
//   payload: {id}
// }
//
// export const removeLabel = (id: string): RemoveLabel => ({
//   type: 'REMOVE_LABEL',
//   payload: {id},
// })

//
// export const deleteLabel = (id: string) => async (
//   dispatch: Dispatch<Action>
// ) => {
//   try {
//     await client.labels.delete(id)
//
//     dispatch(removeLabel(id))
//   } catch (e) {
//     console.log(e)
//     dispatch(notify(deleteLabelFailed()))
//   }
// }
//
