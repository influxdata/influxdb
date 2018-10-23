// Libraries
import {Dispatch} from 'redux'

// APIs
import {
  getOrganizations as getOrganizationsAPI,
  createOrg as createOrgAPI,
  deleteOrg as deleteOrgAPI,
  updateOrg as updateOrgAPI,
} from 'src/organizations/apis'

// Types
import {AppState, Organization} from 'src/types/v2'

type GetStateFunc = () => Promise<AppState>

export enum ActionTypes {
  SetOrgs = 'SET_ORGS',
  AddOrg = 'ADD_ORG',
  RemoveOrg = 'REMOVE_ORG',
  EditOrg = 'EDIT_ORG',
}

export interface SetOrganizations {
  type: ActionTypes.SetOrgs
  payload: {
    organizations: Organization[]
  }
}

export type Actions = SetOrganizations | AddOrg | RemoveOrg | EditOrg

export const setOrgs = (organizations: Organization[]): SetOrganizations => {
  return {
    type: ActionTypes.SetOrgs,
    payload: {organizations},
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
    link: string
  }
}

export const removeOrg = (link: string): RemoveOrg => ({
  type: ActionTypes.RemoveOrg,
  payload: {link},
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
  dispatch: Dispatch<SetOrganizations>,
  getState: GetStateFunc
): Promise<void> => {
  try {
    const {
      links: {orgs},
    } = await getState()
    const organizations = await getOrganizationsAPI(orgs)
    dispatch(setOrgs(organizations))
  } catch (e) {
    console.error(e)
  }
}

export const createOrg = (link: string, org: Partial<Organization>) => async (
  dispatch: Dispatch<AddOrg>
): Promise<void> => {
  try {
    const createdOrg = await createOrgAPI(link, org)
    dispatch(addOrg(createdOrg))
  } catch (e) {
    console.error(e)
  }
}

export const deleteOrg = (link: string) => async (
  dispatch: Dispatch<RemoveOrg>
): Promise<void> => {
  try {
    await deleteOrgAPI(link)
    dispatch(removeOrg(link))
  } catch (e) {
    console.error(e)
  }
}

export const updateOrg = (org: Organization) => async (
  dispatch: Dispatch<EditOrg>
) => {
  try {
    const updatedOrg = await updateOrgAPI(org)
    dispatch(editOrg(updatedOrg))
  } catch (e) {
    console.error(e)
  }
}
