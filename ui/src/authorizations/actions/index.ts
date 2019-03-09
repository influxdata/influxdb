// API
import {client} from 'src/utils/api'
import * as authAPI from 'src/authorizations/apis'

// Types
import {RemoteDataState} from 'src/types'
import {Authorization} from '@influxdata/influx'
import {Dispatch} from 'redux-thunk'

// Actions
import {notify} from 'src/shared/actions/notifications'

import {
  authorizationsGetFailed,
  authorizationCreateFailed,
  authorizationUpdateFailed,
  authorizationDeleteFailed,
} from 'src/shared/copy/v2/notifications'

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

export const editLabel = (authorization: Authorization): EditAuthorization => ({
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

export const getAuthorizations = () => async (dispatch: Dispatch<Action>) => {
  try {
    dispatch(setAuthorizations(RemoteDataState.Loading))

    const authorizations = (await client.authorizations.getAll()) as Authorization[]

    dispatch(setAuthorizations(RemoteDataState.Done, authorizations))
  } catch (e) {
    console.log(e)
    dispatch(setAuthorizations(RemoteDataState.Error))
    dispatch(notify(authorizationsGetFailed()))
  }
}

export const createAuthorization = (auth: Authorization) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const createdAuthorization = await authAPI.createAuthorization(auth)
    dispatch(addAuthorization(createdAuthorization))
  } catch (e) {
    console.log(e)
    dispatch(notify(authorizationCreateFailed()))
    throw e
  }
}

export const updateAuthorization = (authorization: Authorization) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const label = await client.authorizations.update(
      authorization.id,
      authorization
    )

    dispatch(editLabel(label))
  } catch (e) {
    console.log(e)
    dispatch(notify(authorizationUpdateFailed(authorization.id)))
  }
}

export const deleteAuthorization = (id: string, name: string = '') => async (
  dispatch: Dispatch<Action>
) => {
  try {
    await client.authorizations.delete(id)

    dispatch(removeAuthorization(id))
  } catch (e) {
    console.log(e)
    dispatch(notify(authorizationDeleteFailed(name)))
  }
}
