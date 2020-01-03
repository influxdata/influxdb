// Libraries
import HoneyBadger from 'honeybadger-js'
import {NormalizedSchema} from 'normalizr'

// Types
import {Organization, RemoteDataState, OrgEntities} from 'src/types'

// Action Types
export const SET_ORGS = 'SET_ORGS'
export const SET_ORG = 'SET_ORG'
export const ADD_ORG = 'ADD_ORG'
export const REMOVE_ORG = 'REMOVE_ORG'
export const EDIT_ORG = 'EDIT_ORG'

// Action Definitions
export type Action =
  | ReturnType<typeof setOrgs>
  | ReturnType<typeof addOrg>
  | ReturnType<typeof removeOrg>
  | ReturnType<typeof editOrg>
  | ReturnType<typeof setOrg>

export const setOrgs = (
  status: RemoteDataState,
  schema?: NormalizedSchema<OrgEntities, string[]>
) => {
  return {
    type: SET_ORGS,
    schema,
    status,
  } as const
}

export const setOrg = (org: Organization) => {
  HoneyBadger.setContext({
    orgID: org.id,
  })
  return {
    type: SET_ORG,
    org,
  } as const
}

export const addOrg = (schema: NormalizedSchema<OrgEntities, string>) =>
  ({
    type: ADD_ORG,
    schema,
  } as const)

export const removeOrg = (id: string) =>
  ({
    type: REMOVE_ORG,
    id,
  } as const)

export const editOrg = (schema: NormalizedSchema<OrgEntities, string>) =>
  ({
    type: EDIT_ORG,
    schema,
  } as const)
