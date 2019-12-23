// API
import {client} from 'src/utils/api'
import * as api from 'src/client'

// Types
import {RemoteDataState, GetState} from 'src/types'
import {Label, Telegraf} from 'src/client'
import {Dispatch, ThunkAction} from 'redux-thunk'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Utils
import {
  telegrafGetFailed,
  telegrafCreateFailed,
  telegrafUpdateFailed,
  telegrafDeleteFailed,
  addTelelgrafLabelFailed,
  removeTelelgrafLabelFailed,
  getTelegrafConfigFailed,
} from 'src/shared/copy/notifications'

export type Action =
  | SetTelegrafs
  | AddTelegraf
  | EditTelegraf
  | RemoveTelegraf
  | SetCurrentConfig

interface SetTelegrafs {
  type: 'SET_TELEGRAFS'
  payload: {
    status: RemoteDataState
    list: Telegraf[]
  }
}

export const setTelegrafs = (
  status: RemoteDataState,
  list?: Telegraf[]
): SetTelegrafs => ({
  type: 'SET_TELEGRAFS',
  payload: {status, list},
})

interface AddTelegraf {
  type: 'ADD_TELEGRAF'
  payload: {
    telegraf: Telegraf
  }
}

export const addTelegraf = (telegraf: Telegraf): AddTelegraf => ({
  type: 'ADD_TELEGRAF',
  payload: {telegraf},
})

interface EditTelegraf {
  type: 'EDIT_TELEGRAF'
  payload: {
    telegraf: Telegraf
  }
}

export const editTelegraf = (telegraf: Telegraf): EditTelegraf => ({
  type: 'EDIT_TELEGRAF',
  payload: {telegraf},
})

interface RemoveTelegraf {
  type: 'REMOVE_TELEGRAF'
  payload: {id: string}
}

export const removeTelegraf = (id: string): RemoveTelegraf => ({
  type: 'REMOVE_TELEGRAF',
  payload: {id},
})

export interface SetCurrentConfig {
  type: 'SET_CURRENT_CONFIG'
  payload: {status: RemoteDataState; item?: string}
}

export const setCurrentConfig = (
  status: RemoteDataState,
  item?: string
): SetCurrentConfig => ({
  type: 'SET_CURRENT_CONFIG',
  payload: {status, item},
})

export const getTelegrafs = () => async (dispatch, getState: GetState) => {
  const {
    orgs: {org},
  } = getState()

  try {
    dispatch(setTelegrafs(RemoteDataState.Loading))

    const resp = await api.getTelegrafs({query: {orgID: org.id}})

    if (resp.status !== 200) {
      throw new Error("Couldn't get the telegraf config for this organization")
    }

    dispatch(setTelegrafs(RemoteDataState.Done, resp.data.configurations))
  } catch (e) {
    console.error(e)
    dispatch(setTelegrafs(RemoteDataState.Error))
    dispatch(notify(telegrafGetFailed()))
  }
}

export const createTelegraf = (telegraf: Telegraf) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const resp = await api.postTelegraf({data: telegraf})

    if (resp.status !== 201) {
      throw new Error('There was an error creating the telegraf config')
    }

    dispatch(addTelegraf(resp.data))
  } catch (e) {
    console.error(e)
    dispatch(notify(telegrafCreateFailed()))
    throw e
  }
}

export const updateTelegraf = (telegraf: Telegraf) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const resp = await api.putTelegraf({
      telegrafID: telegraf.id,
      data: telegraf,
    })
    if (resp.status !== 200) {
      throw new Error('Failed update the telegraf config')
    }

    dispatch(editTelegraf(resp.data))
  } catch (e) {
    console.error(e)
    dispatch(notify(telegrafUpdateFailed(telegraf.name)))
  }
}

export const deleteTelegraf = (id: string, name: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    await client.telegrafConfigs.delete(id)

    dispatch(removeTelegraf(id))
  } catch (e) {
    console.error(e)
    dispatch(notify(telegrafDeleteFailed(name)))
  }
}

export const addTelelgrafLabelAsync = (
  telegrafID: string,
  label: Label
): ThunkAction<Promise<void>> => async (dispatch): Promise<void> => {
  try {
    await api.postTelegrafsLabel({telegrafID, data: {labelID: label as string}})
    const resp = await api.getTelegraf({telegrafID})

    if (resp.status !== 200) {
      throw new Error(
        'An error occurred while adding labels to the telegraf config'
      )
    }

    dispatch(editTelegraf(resp.data))
  } catch (error) {
    console.error(error)
    dispatch(notify(addTelelgrafLabelFailed()))
  }
}

export const removeTelelgrafLabelAsync = (
  telegrafID: string,
  label: Label
): ThunkAction<Promise<void>> => async (dispatch): Promise<void> => {
  try {
    await api.deleteTelegrafsLabel({telegrafID, labelID: label as string})
    const resp = await api.getTelegraf({telegrafID})

    if (resp.status !== 200) {
      throw new Error(
        'An error occurred while removing labels from the telegraf config'
      )
    }

    dispatch(editTelegraf(resp.data))
  } catch (error) {
    console.error(error)
    dispatch(notify(removeTelelgrafLabelFailed()))
  }
}

export const getTelegrafConfigToml = (telegrafConfigID: string) => async (
  dispatch
): Promise<void> => {
  try {
    dispatch(setCurrentConfig(RemoteDataState.Loading))
    const config = await client.telegrafConfigs.getTOML(telegrafConfigID)
    dispatch(setCurrentConfig(RemoteDataState.Done, config))
  } catch (error) {
    dispatch(setCurrentConfig(RemoteDataState.Error))
    dispatch(notify(getTelegrafConfigFailed()))
  }
}
