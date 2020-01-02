// Libraries
import {get} from 'lodash'
import {normalize, NormalizedSchema} from 'normalizr'

// API
import * as api from 'src/client'
import * as schemas from 'src/schemas'

// Types
import {RemoteDataState, GetState} from 'src/types'
import {AddResourceMemberRequestBody} from '@influxdata/influx'
import {Dispatch} from 'react'
import {Member} from 'src/types'

// Actions
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'
import {
  memberAddSuccess,
  memberAddFailed,
  memberRemoveSuccess,
  memberRemoveFailed,
} from 'src/shared/copy/notifications'

// Selectors
import {getOrg} from 'src/organizations/selectors'

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
  schema?: NormalizedSchema<schemas.MemberEntities, string[]>
) =>
  ({
    type: SET_MEMBERS,
    status,
    schema,
  } as const)

export const addMember = (
  schema: NormalizedSchema<schemas.MemberEntities, string>
) =>
  ({
    type: ADD_MEMBER,
    schema,
  } as const)

export const removeMember = (id: string) =>
  ({
    type: REMOVE_MEMBER,
    id,
  } as const)

export const getMembers = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const {id} = getOrg(getState())
    dispatch(setMembers(RemoteDataState.Loading))

    const [ownersResp, membersResp] = await Promise.all([
      api.getOrgsOwners({orgID: id}),
      api.getOrgsMembers({orgID: id}),
    ])

    if (ownersResp.status !== 200) {
      throw new Error(ownersResp.data.message)
    }

    if (membersResp.status !== 200) {
      throw new Error(membersResp.data.message)
    }

    const owners = ownersResp.data.users

    const members = membersResp.data.users

    const allMembers = [...owners, ...members]

    const normalized = normalize<Member, schemas.MemberEntities, string[]>(
      allMembers,
      [schemas.members]
    )

    dispatch(setMembers(RemoteDataState.Done, normalized))
  } catch (e) {
    console.error(e)
    dispatch(setMembers(RemoteDataState.Error))
  }
}

export const addNewMember = (data: AddResourceMemberRequestBody) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const {id} = getOrg(getState())
    const resp = await api.postOrgsMember({orgID: id, data})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const newMember = resp.data
    const member = normalize<Member, schemas.MemberEntities, string>(
      newMember,
      schemas.members
    )

    dispatch(addMember(member))
    dispatch(notify(memberAddSuccess(newMember.name)))
  } catch (e) {
    console.error(e)
    const message = get(e, 'response.data.message', 'Unknown error')
    dispatch(notify(memberAddFailed(message)))
    throw e
  }
}

export const deleteMember = (member: Member) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const {id} = getOrg(getState())
    const resp = await api.deleteOrgsMember({orgID: id, userID: member.id})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeMember(member.id))

    dispatch(notify(memberRemoveSuccess(member.name)))
  } catch (e) {
    console.error(e)
    dispatch(notify(memberRemoveFailed(member.name)))
  }
}
