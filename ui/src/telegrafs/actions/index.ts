// API
import {client} from 'src/utils/api'

// Types
import {RemoteDataState, GetState} from 'src/types'
import {ITelegraf as Telegraf, ILabel as Label} from '@influxdata/influx'
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

    const telegrafs = await client.telegrafConfigs.getAll(org.id)

    dispatch(setTelegrafs(RemoteDataState.Done, telegrafs))
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
    const createdTelegraf = await client.telegrafConfigs.create(telegraf)
    dispatch(addTelegraf(createdTelegraf))
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
    const t = await client.telegrafConfigs.update(telegraf.id, telegraf)

    dispatch(editTelegraf(t))
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

export const addTelelgrafLabelsAsync = (
  telegrafID: string,
  labels: Label[]
): ThunkAction<Promise<void>> => async (dispatch): Promise<void> => {
  try {
    await client.telegrafConfigs.addLabels(telegrafID, labels)
    const telegraf = await client.telegrafConfigs.get(telegrafID)

    dispatch(editTelegraf(telegraf))
  } catch (error) {
    console.error(error)
    dispatch(notify(addTelelgrafLabelFailed()))
  }
}

export const removeTelelgrafLabelsAsync = (
  telegrafID: string,
  labels: Label[]
): ThunkAction<Promise<void>> => async (dispatch): Promise<void> => {
  try {
    await client.telegrafConfigs.removeLabels(telegrafID, labels)
    const telegraf = await client.telegrafConfigs.get(telegrafID)

    dispatch(editTelegraf(telegraf))
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
