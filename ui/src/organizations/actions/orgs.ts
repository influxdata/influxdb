// Libraries
import {Dispatch} from 'redux'
import {push, RouterAction} from 'react-router-redux'
import HoneyBadger from 'honeybadger-js'

// APIs
import {getErrorMessage} from 'src/utils/api'
import * as api from 'src/client'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Constants
import {
  orgCreateSuccess,
  orgCreateFailed,
  bucketCreateSuccess,
  bucketCreateFailed,
  orgEditSuccess,
  orgEditFailed,
  orgRenameSuccess,
  orgRenameFailed,
} from 'src/shared/copy/notifications'

// Types
import {
  Organization,
  RemoteDataState,
  NotificationAction,
  Bucket,
  AppThunk,
} from 'src/types'

export enum ActionTypes {
  SetOrgs = 'SET_ORGS',
  SetOrgsStatus = 'SET_ORGS_STATUS',
  AddOrg = 'ADD_ORG',
  RemoveOrg = 'REMOVE_ORG',
  EditOrg = 'EDIT_ORG',
  SetOrg = 'SET_ORG',
}

export type Actions =
  | SetOrgs
  | AddOrg
  | RemoveOrg
  | EditOrg
  | SetOrgsStatus
  | SetOrg

export interface SetOrgs {
  type: ActionTypes.SetOrgs
  payload: {
    status: RemoteDataState
    orgs: Organization[]
  }
}

export const setOrgs = (
  orgs: Organization[],
  status: RemoteDataState
): SetOrgs => {
  return {
    type: ActionTypes.SetOrgs,
    payload: {status, orgs},
  }
}

export interface SetOrgsStatus {
  type: ActionTypes.SetOrgsStatus
  payload: {
    status: RemoteDataState
  }
}

export interface SetOrg {
  type: ActionTypes.SetOrg
  payload: {
    org: Organization
  }
}

export const setOrg = (org: Organization): SetOrg => {
  HoneyBadger.setContext({
    orgID: org.id,
  })
  return {
    type: ActionTypes.SetOrg,
    payload: {org},
  }
}

export const setOrgsStatus = (status: RemoteDataState): SetOrgsStatus => {
  return {
    type: ActionTypes.SetOrgsStatus,
    payload: {status},
  }
}

export interface AddOrg {
  type: ActionTypes.AddOrg
  payload: {
    org: Organization
  }
}

export const addOrg = (org: Organization): AddOrg => ({
  type: ActionTypes.AddOrg,
  payload: {org},
})

export interface RemoveOrg {
  type: ActionTypes.RemoveOrg
  payload: {
    org: Organization
  }
}

export const removeOrg = (org: Organization): RemoveOrg => ({
  type: ActionTypes.RemoveOrg,
  payload: {org},
})

export interface EditOrg {
  type: ActionTypes.EditOrg
  payload: {
    org: Organization
  }
}

export const editOrg = (org: Organization): EditOrg => ({
  type: ActionTypes.EditOrg,
  payload: {org},
})

// Async Actions

export const getOrganizations = () => async (
  dispatch
): Promise<Organization[]> => {
  try {
    dispatch(setOrgsStatus(RemoteDataState.Loading))

    const resp = await api.getOrgs({})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const {orgs} = resp.data

    dispatch(setOrgs(orgs, RemoteDataState.Done))

    return orgs
  } catch (e) {
    console.error(e)
    dispatch(setOrgs(null, RemoteDataState.Error))
  }
}

export const createOrgWithBucket = (
  org: Organization,
  bucket: Bucket
): AppThunk<Promise<void>> => async (
  dispatch: Dispatch<Actions | RouterAction | NotificationAction>
) => {
  let createdOrg: Organization

  try {
    const orgResp = await api.postOrg({data: org})
    if (orgResp.status !== 201) {
      throw new Error(orgResp.data.message)
    }
    createdOrg = orgResp.data

    dispatch(notify(orgCreateSuccess()))

    dispatch(addOrg(createdOrg))
    dispatch(push(`/orgs/${createdOrg.id}`))

    const bucketResp = await api.postBucket({
      data: {...bucket, orgID: createdOrg.id},
    })

    if (bucketResp.status !== 201) {
      throw new Error(bucketResp.data.message)
    }

    dispatch(notify(bucketCreateSuccess()))
  } catch (e) {
    console.error(e)

    if (!createdOrg) {
      dispatch(notify(orgCreateFailed()))
    }
    const message = getErrorMessage(e)
    dispatch(notify(bucketCreateFailed(message)))
  }
}

export const createOrg = (org: Organization) => async (
  dispatch: Dispatch<Actions | RouterAction | NotificationAction>
): Promise<void> => {
  try {
    const resp = await api.postOrg({data: org})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const createdOrg = resp.data

    dispatch(addOrg(createdOrg))
    dispatch(push(`/orgs/${createdOrg.id}`))

    dispatch(notify(orgCreateSuccess()))
  } catch (e) {
    console.error(e)
    dispatch(notify(orgCreateFailed()))
  }
}

export const deleteOrg = (org: Organization) => async (
  dispatch: Dispatch<RemoveOrg>
): Promise<void> => {
  try {
    const resp = await api.deleteOrg({orgID: org.id})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeOrg(org))
  } catch (e) {
    console.error(e)
  }
}

export const updateOrg = (org: Organization) => async (
  dispatch: Dispatch<EditOrg | NotificationAction>
) => {
  try {
    const resp = await api.patchOrg({orgID: org.id, data: org})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const updatedOrg = resp.data

    dispatch(editOrg(updatedOrg))

    dispatch(notify(orgEditSuccess()))
  } catch (e) {
    dispatch(notify(orgEditFailed()))
    console.error(e)
  }
}

export const renameOrg = (
  originalName: string,
  org: Organization
): AppThunk<Promise<void>> => async (
  dispatch: Dispatch<EditOrg | NotificationAction>
) => {
  try {
    const resp = await api.patchOrg({orgID: org.id, data: org})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const updatedOrg = resp.data

    dispatch(editOrg(updatedOrg))

    dispatch(notify(orgRenameSuccess(updatedOrg.name)))
  } catch (e) {
    dispatch(notify(orgRenameFailed(originalName)))
    console.error(e)
  }
}
