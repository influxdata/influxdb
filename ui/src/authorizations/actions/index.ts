// Libraries
import {Dispatch} from 'react'

// API
import * as authAPI from 'src/authorizations/apis'
import * as api from 'src/client'

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
import {
  RemoteDataState,
  GetState,
  NotificationAction,
  Authorization,
} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'

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
    const org = getOrg(getState())
    const resp = await api.getAuthorizations({query: {orgID: org.id}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const {authorizations} = resp.data

    dispatch(setAuthorizations(RemoteDataState.Done, authorizations))
  } catch (e) {
    console.error(e)
    dispatch(setAuthorizations(RemoteDataState.Error))
    dispatch(notify(authorizationsGetFailed()))
  }
}

export const getAuthorization = async (authID: string) => {
  try {
    const resp = await api.getAuthorization({authID})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    return resp.data
  } catch (e) {
    console.error(e)
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
    const resp = await api.patchAuthorization({
      authID: authorization.id,
      data: authorization,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

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
    const resp = await api.deleteAuthorization({authID: id})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeAuthorization(id))
    dispatch(notify(authorizationDeleteSuccess()))
  } catch (e) {
    console.error(e)
    dispatch(notify(authorizationDeleteFailed(name)))
  }
}
