// API
import {client} from 'src/utils/api'

// Types
import {Dispatch} from 'react'
import {RemoteDataState, AppThunk} from 'src/types'
import {ILabel, ILabelProperties} from '@influxdata/influx'
import {LabelProperties} from 'src/types/labels'

// Actions
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'
import {
  getLabelsFailed,
  createLabelFailed,
  updateLabelFailed,
  deleteLabelFailed,
} from 'src/shared/copy/notifications'
import {GetState} from 'src/types'

export type Action =
  | SetLabels
  | AddLabel
  | EditLabel
  | RemoveLabel
  | NotifyAction

interface SetLabels {
  type: 'SET_LABELS'
  payload: {
    status: RemoteDataState
    list: ILabel[]
  }
}

export const setLabels = (
  status: RemoteDataState,
  list?: ILabel[]
): SetLabels => ({
  type: 'SET_LABELS',
  payload: {status, list},
})

interface AddLabel {
  type: 'ADD_LABEL'
  payload: {
    label: ILabel
  }
}

export const addLabel = (label: ILabel): AddLabel => ({
  type: 'ADD_LABEL',
  payload: {label},
})

interface EditLabel {
  type: 'EDIT_LABEL'
  payload: {label}
}

export const editLabel = (label: ILabel): EditLabel => ({
  type: 'EDIT_LABEL',
  payload: {label},
})

interface RemoveLabel {
  type: 'REMOVE_LABEL'
  payload: {id}
}

export const removeLabel = (id: string): RemoveLabel => ({
  type: 'REMOVE_LABEL',
  payload: {id},
})

export const getLabels = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const {
      orgs: {org},
    } = getState()
    dispatch(setLabels(RemoteDataState.Loading))

    const labels = await client.labels.getAll(org.id)

    dispatch(setLabels(RemoteDataState.Done, labels))
  } catch (e) {
    console.error(e)
    dispatch(setLabels(RemoteDataState.Error))
    dispatch(notify(getLabelsFailed()))
  }
}

export const createLabel = (
  name: string,
  properties: LabelProperties
): AppThunk<Promise<void>> => async (
  dispatch: Dispatch<Action>,
  getState: GetState
): Promise<void> => {
  const {
    orgs: {org},
  } = getState()

  try {
    const createdLabel = await client.labels.create({
      orgID: org.id,
      name,
      properties: properties as ILabelProperties,
    })

    dispatch(addLabel(createdLabel))
  } catch (e) {
    console.error(e)
    dispatch(notify(createLabelFailed()))
  }
}

export const updateLabel = (id: string, l: ILabel) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const label = await client.labels.update(id, l)

    dispatch(editLabel(label))
  } catch (e) {
    console.error(e)
    dispatch(notify(updateLabelFailed()))
  }
}

export const deleteLabel = (id: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    await client.labels.delete(id)

    dispatch(removeLabel(id))
  } catch (e) {
    console.error(e)
    dispatch(notify(deleteLabelFailed()))
  }
}
