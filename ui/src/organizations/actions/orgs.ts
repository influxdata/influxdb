// Libraries
import {Dispatch} from 'redux'
import {push, RouterAction} from 'react-router-redux'

// APIs
import {client} from 'src/utils/api'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Constants
import {defaultTemplates} from 'src/templates/constants/'
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
import {Bucket} from '@influxdata/influx'
import {Organization, RemoteDataState, NotificationAction} from 'src/types'

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

    const organizations = await client.organizations.getAll()
    dispatch(setOrgs(organizations, RemoteDataState.Done))
    return organizations
  } catch (e) {
    console.error(e)
    dispatch(setOrgs(null, RemoteDataState.Error))
  }
}

export const createOrgWithBucket = (
  org: Organization,
  bucket: Bucket
) => async (
  dispatch: Dispatch<Actions | RouterAction | NotificationAction>
) => {
  let createdOrg: Organization

  try {
    createdOrg = await client.organizations.create(org)
    await client.templates.create({
      ...defaultTemplates.systemTemplate(),
      orgID: createdOrg.id,
    })
    await client.templates.create({
      ...defaultTemplates.gettingStartedWithFluxTemplate(),
      orgID: createdOrg.id,
    })
    dispatch(notify(orgCreateSuccess()))

    dispatch(addOrg(createdOrg))
    dispatch(push(`/orgs/${createdOrg.id}`))

    await client.buckets.create({
      ...bucket,
      orgID: createdOrg.id,
    })

    dispatch(notify(bucketCreateSuccess()))
  } catch (e) {
    console.error(e)

    if (!createdOrg) {
      dispatch(notify(orgCreateFailed()))
    }
    dispatch(notify(bucketCreateFailed()))
  }
}

export const createOrg = (org: Organization) => async (
  dispatch: Dispatch<Actions | RouterAction | NotificationAction>
): Promise<void> => {
  try {
    const createdOrg = await client.organizations.create(org)
    await client.templates.create({
      ...defaultTemplates.systemTemplate(),
      orgID: createdOrg.id,
    })

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
    await client.organizations.delete(org.id)
    dispatch(removeOrg(org))
  } catch (e) {
    console.error(e)
  }
}

export const updateOrg = (org: Organization) => async (
  dispatch: Dispatch<EditOrg | NotificationAction>
) => {
  try {
    const updatedOrg = await client.organizations.update(org.id, org)
    dispatch(editOrg(updatedOrg))
    dispatch(notify(orgEditSuccess()))
  } catch (e) {
    dispatch(notify(orgEditFailed()))
    console.error(e)
  }
}

export const renameOrg = (originalName: string, org: Organization) => async (
  dispatch: Dispatch<EditOrg | NotificationAction>
) => {
  try {
    const updatedOrg = await client.organizations.update(org.id, org)
    dispatch(editOrg(updatedOrg))
    dispatch(notify(orgRenameSuccess(updatedOrg.name)))
  } catch (e) {
    dispatch(notify(orgRenameFailed(originalName)))
    console.error(e)
  }
}
