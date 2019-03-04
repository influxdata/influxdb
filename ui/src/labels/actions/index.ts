// API
import {client} from 'src/utils/api'

// Types
import {RemoteDataState} from 'src/types'
import {Label} from 'src/types/v2'
import {LabelProperties} from 'src/types/v2/labels'
import {Dispatch} from 'redux-thunk'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {getLabelsFailed, createLabelFailed} from 'src/shared/copy/notifications'

export type Action = SetLabels | AddLabel

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

interface AddLabel {
  type: 'ADD_LABEL'
  payload: {
    label: Label
  }
}

export const addLabel = (label: Label): AddLabel => ({
  type: 'ADD_LABEL',
  payload: {label},
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

export const createLabel = (
  name: string,
  properties: LabelProperties
) => async (dispatch: Dispatch<Action>) => {
  try {
    const createdLabel = await client.labels.create(name, properties)

    dispatch(addLabel(createdLabel))
  } catch (e) {
    console.log(e)
    dispatch(notify(createLabelFailed()))
  }
}
