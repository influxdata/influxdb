// Libraries
import _ from 'lodash'
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

export type Action = ReturnType<typeof setMembers>
// | AddMember
// | RemoveMember
// | NotifyAction

export const SET_MEMBERS = 'SET_MEMBERS'

export const setMembers = (
  status: RemoteDataState,
  normalized?: NormalizedSchema<schemas.MemberEntities, string[]>
) =>
  ({
    type: SET_MEMBERS,
    status,
    normalized,
  } as const)

interface AddMember {
  type: 'ADD_MEMBER'
  payload: {
    member: Member
  }
}

export const addMember = (member: Member): AddMember => ({
  type: 'ADD_MEMBER',
  payload: {member},
})

interface RemoveMember {
  type: 'REMOVE_MEMBER'
  payload: {id: string}
}

export const removeMember = (id: string): RemoveMember => ({
  type: 'REMOVE_MEMBER',
  payload: {id},
})

export const getMembers = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const {
      orgs: {
        org: {id},
      },
    } = getState()
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

    const normalized = normalize<any, schemas.MemberEntities, string[]>(
      allMembers,
      [schemas.members]
    )

    dispatch(setMembers(RemoteDataState.Done, normalized))
  } catch (e) {
    console.error(e)
    dispatch(setMembers(RemoteDataState.Error))
  }
}

export const addNewMember = (member: AddResourceMemberRequestBody) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const {
      orgs: {
        org: {id},
      },
    } = getState()

    const resp = await api.postOrgsMember({orgID: id, data: member})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const newMember = resp.data

    dispatch(addMember(newMember))
    dispatch(notify(memberAddSuccess(member.name)))
  } catch (e) {
    console.error(e)
    const message = _.get(e, 'response.data.message', 'Unknown error')
    dispatch(notify(memberAddFailed(message)))
    throw e
  }
}

export const deleteMember = (member: Member) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const {
      orgs: {
        org: {id},
      },
    } = getState()

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
