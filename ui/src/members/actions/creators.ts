// Actions
import {Action as NotifyAction} from 'src/shared/actions/notifications'

// Types
import {NormalizedSchema} from 'normalizr'
import {MemberEntities, RemoteDataState} from 'src/types'

export type Action =
  | ReturnType<typeof setMembers>
  | ReturnType<typeof addMember>
  | ReturnType<typeof removeMember>
  | NotifyAction

export const SET_MEMBERS = 'SET_MEMBERS'
export const ADD_MEMBER = 'ADD_MEMBER'
export const REMOVE_MEMBER = 'REMOVE_MEMBER'

export const setMembers = (
  status: RemoteDataState,
  schema?: NormalizedSchema<MemberEntities, string[]>
) =>
  ({
    type: SET_MEMBERS,
    status,
    schema,
  } as const)

export const addMember = (schema: NormalizedSchema<MemberEntities, string>) =>
  ({
    type: ADD_MEMBER,
    schema,
  } as const)

export const removeMember = (id: string) =>
  ({
    type: REMOVE_MEMBER,
    id,
  } as const)
