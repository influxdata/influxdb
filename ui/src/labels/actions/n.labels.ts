// Libraries
import {normalize, NormalizedSchema} from 'normalizr'

// API
import {client} from 'src/utils/api'

// Schemas
import * as schemas from 'src/schemas'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {getLabelsFailed} from 'src/shared/copy/notifications'

// Types
import {GetState, RemoteDataState, Entities, AppThunk} from 'src/types'
import {Dispatch} from 'react-redux'

export const SET_LABELS = 'N_SET_LABELS'

interface SetLabelsAction {
  type: typeof SET_LABELS
  status: RemoteDataState
  normalized?: NormalizedSchema<Entities, string[]>
}

export type LabelActionTypes = SetLabelsAction

export const setLabels = (
  status: RemoteDataState,
  normalized?: NormalizedSchema<Entities, string[]>
): LabelActionTypes => {
  return {
    type: SET_LABELS,
    status,
    normalized,
  }
}

export const getLabels = () => async (
  dispatch: Dispatch<LabelActionTypes>,
  getState: GetState
): AppThunk => {
  try {
    const {
      orgs: {org},
    } = getState()
    dispatch(setLabels(RemoteDataState.Loading))

    const labels = await client.labels.getAll(org.id)
    const normalized = normalize<any, Entities, string[]>(labels, [
      schemas.labels,
    ])

    dispatch(setLabels(RemoteDataState.Done, normalized))
  } catch (e) {
    console.error(e)
    dispatch(setLabels(RemoteDataState.Error))
    dispatch(notify(getLabelsFailed()))
  }
}
