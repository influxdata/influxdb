// Libraries
import {normalize} from 'normalizr'

// API
import {
  getLabels as apiGetLabels,
  postLabel as apiPostLabel,
  patchLabel as apiPatchLabel,
  deleteLabel as apiDeleteLabel,
} from 'src/client'

// Schemas
import {labelSchema, arrayOfLabels} from 'src/schemas/labels'

// Types
import {Dispatch} from 'react'
import {
  RemoteDataState,
  LabelProperties,
  GetState,
  Label,
  LabelEntities,
  ResourceType,
} from 'src/types'

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
  removeLabel,
  Action,
} from 'src/labels/actions/creators'

// Utils
import {getOrg} from 'src/organizations/selectors'
import {viewableLabels} from 'src/labels/selectors'
import {getStatus} from 'src/resources/selectors'

export const getLabels = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const state = getState()
    if (getStatus(state, ResourceType.Labels) === RemoteDataState.NotStarted) {
      dispatch(setLabels(RemoteDataState.Loading))
    }

    const org = getOrg(state)

    const resp = await apiGetLabels({query: {orgID: org.id}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const labels = normalize<Label, LabelEntities, string[]>(
      viewableLabels(resp.data.labels),
      arrayOfLabels
    )

    dispatch(setLabels(RemoteDataState.Done, labels))
  } catch (error) {
    console.error(error)
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

    const label = normalize<Label, LabelEntities, string>(
      resp.data.label,
      labelSchema
    )

    dispatch(setLabel(resp.data.label.id, RemoteDataState.Done, label))
  } catch (error) {
    console.error(error)
    dispatch(notify(createLabelFailed()))
  }
}

export const updateLabel = (id: string, l: Label) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    dispatch(setLabel(id, RemoteDataState.Loading))
    const resp = await apiPatchLabel({labelID: id, data: l})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const label = normalize<Label, LabelEntities, string>(
      resp.data.label,
      labelSchema
    )

    dispatch(setLabel(id, RemoteDataState.Done, label))
  } catch (error) {
    console.error(error)
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
  } catch (error) {
    console.error(error)
    dispatch(notify(deleteLabelFailed()))
  }
}
