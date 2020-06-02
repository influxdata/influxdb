// Libraries
import {Dispatch} from 'react'

// API
import {
  postOrgsInvitesResend,
  deleteOrgsInvite,
  deleteOrgsUser,
} from 'src/client/unifyRoutes'

// Actions
import {
  updateUser,
  updateInvite,
  removeInvite,
  removeUser as removeUserAction,
  removeUserStatus,
  removeInviteStatus,
  resendInviteStatus,
  Action,
} from '../reducers'
import {RemoteDataState} from '@influxdata/clockface'

// Types
import {Invite, CloudUser} from 'src/types'

export const resendInvite = async (
  dispatch: Dispatch<Action>,
  orgID: string,
  inviteID: string
) => {
  try {
    dispatch(resendInviteStatus(RemoteDataState.Loading))
    const resp = await postOrgsInvitesResend({orgID, inviteID})

    if (resp.status !== 200) {
      throw Error(resp.data.message)
    }

    const invite: Invite = {...resp.data, status: RemoteDataState.Done}

    dispatch(updateInvite(invite))
    dispatch(resendInviteStatus(RemoteDataState.Done))
  } catch (error) {
    dispatch(resendInviteStatus(RemoteDataState.Error))
    console.error(error)
  }
}

export const withdrawInvite = async (
  dispatch: Dispatch<Action>,
  orgID: string,
  invite: Invite
) => {
  try {
    dispatch(updateInvite({...invite, status: RemoteDataState.Loading}))

    const resp = await deleteOrgsInvite({orgID, inviteID: invite.id})

    if (resp.status !== 204) {
      throw Error(resp.data.message)
    }

    dispatch(removeInvite(invite.id))
    dispatch(removeInviteStatus(RemoteDataState.Done))
  } catch (error) {
    console.error(error)

    dispatch(updateInvite({...invite, status: RemoteDataState.Error}))
    dispatch(removeInviteStatus(RemoteDataState.Error))
  }
}

export const removeUser = async (
  dispatch: Dispatch<Action>,
  user: CloudUser,
  orgID: string
) => {
  try {
    dispatch(updateUser({...user, status: RemoteDataState.Loading}))

    const resp = await deleteOrgsUser({orgID, userID: user.id})

    if (resp.status !== 204) {
      throw Error(resp.data.message)
    }

    dispatch(removeUserAction(user.id))
    dispatch(removeUserStatus(RemoteDataState.Done))
  } catch (error) {
    console.error(error)

    dispatch(updateUser({...user, status: RemoteDataState.Error}))
    dispatch(removeUserStatus(RemoteDataState.Error))
  }
}
