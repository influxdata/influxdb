// API
import {
  getLabels as apiGetLabels,
  postLabel as apiPostLabel,
  patchLabel as apiPatchLabel,
  deleteLabel as apiDeleteLabel,
} from 'src/client'

// Types
import {Dispatch} from 'react'
import {RemoteDataState, LabelProperties, GetState, Label} from 'src/types'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {
  getLabelsFailed,
  createLabelFailed,
  updateLabelFailed,
  deleteLabelFailed,
} from 'src/shared/copy/notifications'
import {
  setLabels,
  setLabel,
  editLabel,
  removeLabel,
  Action,
} from 'src/labels/actions/creators'

// Utils
import {addLabelDefaults} from 'src/labels/utils/'
import {getOrg} from 'src/organizations/selectors'

export const getLabels = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const org = getOrg(getState())
    dispatch(setLabels(RemoteDataState.Loading))

    const resp = await apiGetLabels({query: {orgID: org.id}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const labels = resp.data.labels.map(l => addLabelDefaults(l))

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
) => async (dispatch: Dispatch<Action>, getState: GetState): Promise<void> => {
  const org = getOrg(getState())
  try {
    const resp = await apiPostLabel({
      data: {
        orgID: org.id,
        name,
        properties,
      },
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const createdLabel = addLabelDefaults(resp.data.label)

    dispatch(setLabel(createdLabel))
  } catch (e) {
    console.error(e)
    dispatch(notify(createLabelFailed()))
  }
}

export const updateLabel = (id: string, l: Label) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const resp = await apiPatchLabel({labelID: id, data: l})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const label = addLabelDefaults(resp.data.label)

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
    const resp = await apiDeleteLabel({labelID: id})
    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }
    dispatch(removeLabel(id))
  } catch (e) {
    console.error(e)
    dispatch(notify(deleteLabelFailed()))
  }
}
