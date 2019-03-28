// Libraries
import {Dispatch} from 'redux'

// APIs
import {client} from 'src/utils/api'

// Types
import {Organization, RemoteDataState} from 'src/types'

import {defaultTemplates} from 'src/templates/constants/'

export enum ActionTypes {
  SetOrgs = 'SET_ORGS',
  SetOrgsStatus = 'SET_ORGS_STATUS',
  AddOrg = 'ADD_ORG',
  RemoveOrg = 'REMOVE_ORG',
  EditOrg = 'EDIT_ORG',
}

export type Actions = SetOrgs | AddOrg | RemoveOrg | EditOrg | SetOrgsStatus

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

export const createOrg = (org: Organization) => async (
  dispatch: Dispatch<AddOrg>
): Promise<void> => {
  try {
    const createdOrg = await client.organizations.create(org)
    await client.templates.create({
      ...defaultTemplates.systemTemplate(),
      orgID: createdOrg.id,
    })
    dispatch(addOrg(createdOrg))
  } catch (e) {
    console.error(e)
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
  dispatch: Dispatch<EditOrg>
) => {
  try {
    const updatedOrg = await client.organizations.update(org.id, org)
    dispatch(editOrg(updatedOrg))
  } catch (e) {
    console.error(e)
  }
}
