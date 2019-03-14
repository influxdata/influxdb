// API
import {client} from 'src/utils/api'

// Types
import {RemoteDataState} from 'src/types'
import {Telegraf} from '@influxdata/influx'
import {Dispatch} from 'redux-thunk'

// Actions
import {notify} from 'src/shared/actions/notifications'

import {
  telegrafGetFailed,
  telegrafCreateFailed,
  telegrafUpdateFailed,
  telegrafDeleteFailed,
} from 'src/shared/copy/v2/notifications'

export type Action = SetTelegrafs | AddTelegraf | EditTelegraf | RemoveTelegraf

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

export const getTelegrafs = () => async (dispatch: Dispatch<Action>) => {
  try {
    dispatch(setTelegrafs(RemoteDataState.Loading))

    const telegrafs = await client.telegrafConfigs.getAll()

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
