// API
import {client} from 'src/utils/api'

// Types
import {RemoteDataState} from 'src/types'
import {Label} from 'src/types/v2'
import {Dispatch} from 'redux-thunk'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {getLabelsFailed} from 'src/shared/copy/notifications'

export type Action = SetLabels

interface SetLabels {
  type: 'SET_LABELS'
  payload: {
    status: RemoteDataState
    list: Label[]
  }
}

export const setLabels = (
  status: RemoteDataState,
  list?: Label[]
): SetLabels => ({
  type: 'SET_LABELS',
  payload: {status, list},
})

export const getLabels = () => async (dispatch: Dispatch<Action>) => {
  try {
    dispatch(setLabels(RemoteDataState.Loading))

    const labels = await client.labels.getAll()

    dispatch(setLabels(RemoteDataState.Done, labels))
  } catch (e) {
    console.log(e)
    dispatch(setLabels(RemoteDataState.Error))
    dispatch(notify(getLabelsFailed()))
  }
}
