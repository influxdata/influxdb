// Libraries
import {Dispatch} from 'react'

// API
import {client} from 'src/utils/api'
import * as authAPI from 'src/authorizations/apis'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Constants
import {
  authorizationsGetFailed,
  authorizationCreateFailed,
  authorizationUpdateFailed,
  authorizationDeleteFailed,
  authorizationCreateSuccess,
  authorizationDeleteSuccess,
  authorizationUpdateSuccess,
} from 'src/shared/copy/notifications'

// Types
import {RemoteDataState, GetState, NotificationAction} from 'src/types'
import {Authorization} from '@influxdata/influx'

export type Action =
  | SetAuthorizations
  | AddAuthorization
  | EditAuthorization
  | RemoveAuthorization

interface SetAuthorizations {
  type: 'SET_AUTHS'
  payload: {
    status: RemoteDataState
    list: Authorization[]
  }
}

export const setAuthorizations = (
  status: RemoteDataState,
  list?: Authorization[]
): SetAuthorizations => ({
  type: 'SET_AUTHS',
  payload: {status, list},
})

interface AddAuthorization {
  type: 'ADD_AUTH'
  payload: {
    authorization: Authorization
  }
}

export const addAuthorization = (
  authorization: Authorization
): AddAuthorization => ({
  type: 'ADD_AUTH',
  payload: {authorization},
})

interface EditAuthorization {
  type: 'EDIT_AUTH'
  payload: {
    authorization: Authorization
  }
}

export const editAuthorization = (
  authorization: Authorization
): EditAuthorization => ({
  type: 'EDIT_AUTH',
  payload: {authorization},
})

interface RemoveAuthorization {
  type: 'REMOVE_AUTH'
  payload: {id: string}
}

export const removeAuthorization = (id: string): RemoveAuthorization => ({
  type: 'REMOVE_AUTH',
  payload: {id},
})

type GetAuthorizations = (
  dispatch: Dispatch<Action | NotificationAction>,
  getState: GetState
) => Promise<void>
export const getAuthorizations = () => async (
  dispatch: Dispatch<Action | NotificationAction>,
  getState: GetState
) => {
  try {
    dispatch(setAuthorizations(RemoteDataState.Loading))
    const {
      orgs: {org},
    } = getState()

    const authorizations = await client.authorizations.getAll(org.id)

    dispatch(setAuthorizations(RemoteDataState.Done, authorizations))
  } catch (e) {
    console.error(e)
    dispatch(setAuthorizations(RemoteDataState.Error))
    dispatch(notify(authorizationsGetFailed()))
  }
}

export const createAuthorization = (auth: Authorization) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const createdAuthorization = await authAPI.createAuthorization(auth)
    dispatch(addAuthorization(createdAuthorization))
    dispatch(notify(authorizationCreateSuccess()))
  } catch (e) {
    console.error(e)
    dispatch(notify(authorizationCreateFailed()))
    throw e
  }
}

export const updateAuthorization = (authorization: Authorization) => async (
  dispatch: Dispatch<Action | NotificationAction | GetAuthorizations>
) => {
  try {
    await client.authorizations.update(authorization.id, authorization)

    dispatch(getAuthorizations())
    dispatch(notify(authorizationUpdateSuccess()))
  } catch (e) {
    console.error(e)
    dispatch(notify(authorizationUpdateFailed(authorization.id)))
  }
}

export const deleteAuthorization = (id: string, name: string = '') => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    await client.authorizations.delete(id)

    dispatch(removeAuthorization(id))
    dispatch(notify(authorizationDeleteSuccess()))
  } catch (e) {
    console.error(e)
    dispatch(notify(authorizationDeleteFailed(name)))
  }
}
