// Libraries
import {get} from 'lodash'

// API
import {client} from 'src/utils/api'

// Types
import {RemoteDataState} from 'src/types'
import {Bucket} from '@influxdata/influx'
import {Dispatch} from 'redux-thunk'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {
  getBucketsFailed,
  createBucketFailed,
} from 'src/shared/copy/notifications'
// import {
//   bucketDeleteSuccess,
//   bucketDeleteFailed,
//   bucketCreateFailed,
//   bucketCreateSuccess,
//   bucketUpdateFailed,
//   bucketUpdateSuccess,
// } from 'src/shared/copy/v2/notifications'

export type Action = SetBuckets | AddBucket

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
    dispatch(notify(createBucketFailed()))
    throw e
  }
}

// interface EditLabel {
//   type: 'EDIT_LABEL'
//   payload: {label}
// }
//
// export const editLabel = (label: Label): EditLabel => ({
//   type: 'EDIT_LABEL',
//   payload: {label},
// })
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
// export const updateLabel = (id: string, properties: LabelProperties) => async (
//   dispatch: Dispatch<Action>
// ) => {
//   try {
//     const label = await client.labels.update(id, properties)
//
//     dispatch(editLabel(label))
//   } catch (e) {
//     console.log(e)
//     dispatch(notify(updateLabelFailed()))
//   }
// }
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
