// Constants & Types
import {draftInvite} from 'src/users/constants'
import {CloudUser, Invite, DraftInvite} from 'src/types'
import {RemoteDataState} from '@influxdata/clockface'

export interface UserListState {
  users: CloudUser[]
  invites: Invite[]
  draftInvite: DraftInvite
  orgID?: string
  currentUserID: string
  removeUserStatus: RemoteDataState
  removeInviteStatus: RemoteDataState
  resendInviteStatus: RemoteDataState
}

export const resetDraftInvite = () =>
  ({
    type: 'RESET_DRAFT_INVITE',
  } as const)

export const editDraftInvite = (draftInvite: DraftInvite) =>
  ({
    type: 'EDIT_DRAFT_INVITE',
    draftInvite,
  } as const)

export const setInvites = (invites: Invite[]) =>
  ({
    type: 'SET_INVITES',
    invites,
  } as const)

export const removeUser = (id: string) =>
  ({
    type: 'REMOVE_USER',
    id,
  } as const)

export const removeUserStatus = (status: RemoteDataState) =>
  ({
    type: 'REMOVE_USER_STATUS',
    status,
  } as const)

export const removeInvite = (id: string) =>
  ({
    type: 'REMOVE_INVITE',
    id,
  } as const)

export const removeInviteStatus = (status: RemoteDataState) =>
  ({
    type: 'REMOVE_INVITE_STATUS',
    status,
  } as const)

export const resendInviteStatus = (status: RemoteDataState) =>
  ({
    type: 'RESEND_INVITE_STATUS',
    status,
  } as const)

export const updateInvite = (invite: Invite) =>
  ({
    type: 'UPDATE_INVITE',
    invite,
  } as const)

export const updateUser = (user: CloudUser) =>
  ({
    type: 'UPDATE_USER',
    user,
  } as const)

export type Action =
  | ReturnType<typeof editDraftInvite>
  | ReturnType<typeof setInvites>
  | ReturnType<typeof removeUser>
  | ReturnType<typeof removeInvite>
  | ReturnType<typeof resetDraftInvite>
  | ReturnType<typeof updateInvite>
  | ReturnType<typeof updateUser>
  | ReturnType<typeof removeUserStatus>
  | ReturnType<typeof removeInviteStatus>
  | ReturnType<typeof resendInviteStatus>

export type UserListReducer = React.Reducer<UserListState, Action>

export const initialState = ({
  invites = [],
  users = [],
  orgID = null,
  currentUserID = null,
  removeUserStatus = RemoteDataState.NotStarted,
  removeInviteStatus = RemoteDataState.NotStarted,
  resendInviteStatus = RemoteDataState.NotStarted,
}): UserListState => ({
  users,
  invites,
  orgID,
  draftInvite,
  currentUserID,
  removeUserStatus,
  removeInviteStatus,
  resendInviteStatus,
})

export const userListReducer = (
  state: UserListState = initialState({}),
  action: Action
): UserListState => {
  switch (action.type) {
    case 'EDIT_DRAFT_INVITE': {
      return {...state, draftInvite: action.draftInvite}
    }

    case 'SET_INVITES': {
      return {...state, invites: action.invites}
    }

    case 'REMOVE_USER': {
      return {...state, users: state.users.filter(({id}) => id !== action.id)}
    }

    case 'REMOVE_USER_STATUS': {
      return {...state, removeUserStatus: action.status}
    }

    case 'REMOVE_INVITE_STATUS': {
      return {...state, removeInviteStatus: action.status}
    }

    case 'RESEND_INVITE_STATUS': {
      return {...state, resendInviteStatus: action.status}
    }

    case 'REMOVE_INVITE': {
      return {
        ...state,
        invites: state.invites.filter(({id}) => id !== action.id),
      }
    }

    case 'RESET_DRAFT_INVITE': {
      return {...state, draftInvite}
    }

    case 'UPDATE_INVITE': {
      return {
        ...state,
        invites: state.invites.map(invite =>
          invite.id == action.invite.id ? action.invite : invite
        ),
      }
    }

    case 'UPDATE_USER': {
      return {
        ...state,
        users: state.users.map(user =>
          user.id == action.user.id ? action.user : user
        ),
      }
    }

    default: {
      return state
    }
  }
}
