// Libraries
import {NormalizedSchema} from 'normalizr'

// Types
import {RemoteDataState, TelegrafEntities} from 'src/types'
import {Action as NotifyAction} from 'src/shared/actions/notifications'

export const SET_TELEGRAFS = 'SET_TELEGRAFS'
export const ADD_TELEGRAF = 'ADD_TELEGRAF'
export const EDIT_TELEGRAF = 'EDIT_TELEGRAF'
export const REMOVE_TELEGRAF = 'REMOVE_TELEGRAF'
export const SET_CURRENT_CONFIG = 'SET_CURRENT_CONFIG'

export type Action =
  | ReturnType<typeof setTelegrafs>
  | ReturnType<typeof addTelegraf>
  | ReturnType<typeof editTelegraf>
  | ReturnType<typeof removeTelegraf>
  | ReturnType<typeof setCurrentConfig>
  | NotifyAction

export const setTelegrafs = (
  status: RemoteDataState,
  schema?: NormalizedSchema<TelegrafEntities, string[]>
) =>
  ({
    type: SET_TELEGRAFS,
    status,
    schema,
  } as const)

export const addTelegraf = (
  schema: NormalizedSchema<TelegrafEntities, string>
) =>
  ({
    type: ADD_TELEGRAF,
    schema,
  } as const)

export const editTelegraf = (
  schema: NormalizedSchema<TelegrafEntities, string>
) =>
  ({
    type: EDIT_TELEGRAF,
    schema,
  } as const)

export const removeTelegraf = (id: string) =>
  ({
    type: REMOVE_TELEGRAF,
    id,
  } as const)

export const setCurrentConfig = (status: RemoteDataState, item?: string) =>
  ({
    type: SET_CURRENT_CONFIG,
    status,
    item,
  } as const)
