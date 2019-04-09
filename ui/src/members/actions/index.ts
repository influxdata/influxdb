// Libraries
import _ from 'lodash'

// API
import {client} from 'src/utils/api'

// Types
import {RemoteDataState, GetState} from 'src/types'
import {AddResourceMemberRequestBody} from '@influxdata/influx'
import {Dispatch} from 'redux-thunk'
import {Member} from 'src/types'

// Actions
import {notify} from 'src/shared/actions/notifications'

import {
  memberAddSuccess,
  memberAddFailed,
  memberRemoveSuccess,
  memberRemoveFailed,
} from 'src/shared/copy/v2/notifications'

export type Action = SetMembers | AddMember | RemoveMember

interface SetMembers {
  type: 'SET_MEMBERS'
  payload: {
    status: RemoteDataState
    list: Member[]
  }
}

export const setMembers = (
  status: RemoteDataState,
  list?: Member[]
): SetMembers => ({
  type: 'SET_MEMBERS',
  payload: {status, list},
})

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

    const [owners, members] = await Promise.all([
      client.organizations.owners(id),
      client.organizations.members(id),
    ])

    const users = [...owners, ...members]

    dispatch(setMembers(RemoteDataState.Done, users))
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

    const newMember = await client.organizations.addMember(id, member)

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

    await client.organizations.removeMember(id, member.id)
    dispatch(removeMember(member.id))

    dispatch(notify(memberRemoveSuccess(member.name)))
  } catch (e) {
    console.error(e)
    dispatch(notify(memberRemoveFailed(member.name)))
  }
}
