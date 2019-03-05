// API
import {client} from 'src/utils/api'

// Types
import {RemoteDataState} from 'src/types'
import {Bucket} from 'src/types/v2'
import {Dispatch} from 'redux-thunk'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {getBucketsFailed} from 'src/shared/copy/notifications'

export type Action = SetBuckets

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

// interface AddLabel {
//   type: 'ADD_LABEL'
//   payload: {
//     label: Label
//   }
// }
//
// export const addLabel = (label: Label): AddLabel => ({
//   type: 'ADD_LABEL',
//   payload: {label},
// })
//
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

// export const createLabel = (
//   name: string,
//   properties: LabelProperties
// ) => async (dispatch: Dispatch<Action>) => {
//   try {
//     const createdLabel = await client.labels.create(name, properties)
//
//     dispatch(addLabel(createdLabel))
//   } catch (e) {
//     console.log(e)
//     dispatch(notify(createLabelFailed()))
//   }
// }
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
