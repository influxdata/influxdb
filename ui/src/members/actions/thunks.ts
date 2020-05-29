// Libraries
import {Dispatch} from 'react'
import {get} from 'lodash'
import {normalize} from 'normalizr'

// API
import * as api from 'src/client'
import {memberSchema, arrayOfMembers} from 'src/schemas'

// Types
import {
  RemoteDataState,
  GetState,
  Member,
  MemberEntities,
  ResourceType,
} from 'src/types'
import {AddResourceMemberRequestBody} from '@influxdata/influx'

// Actions
import {
  setMembers,
  Action,
  addMember,
  removeMember,
} from 'src/members/actions/creators'
import {notify} from 'src/shared/actions/notifications'
import {
  memberAddSuccess,
  memberAddFailed,
  memberRemoveSuccess,
  memberRemoveFailed,
} from 'src/shared/copy/notifications'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {getStatus} from 'src/resources/selectors'

export const getMembers = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const state = getState()
    if (getStatus(state, ResourceType.Members) === RemoteDataState.NotStarted) {
      dispatch(setMembers(RemoteDataState.Loading))
    }

    const {id} = getOrg(state)

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

    const normalized = normalize<Member, MemberEntities, string[]>(
      allMembers,
      arrayOfMembers
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
    const member = normalize<Member, MemberEntities, string>(
      newMember,
      memberSchema
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
