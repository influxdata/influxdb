import {
  MEMBER_ROLE,
  VIEWER_ROLE,
  EDITOR_ROLE,
  ADMIN_ROLE,
} from 'src/auth/Authorized'

export const USER_ROLES = [
  {name: MEMBER_ROLE},
  {name: VIEWER_ROLE},
  {name: EDITOR_ROLE},
  {name: ADMIN_ROLE},
]
export const DEFAULT_ORG_ID = '0'
export const DEFAULT_ORG_NAME = '__default'
export const DEFAULT_ORG = {
  id: DEFAULT_ORG_ID,
  name: DEFAULT_ORG_NAME,
}
export const NO_ORG = 'No Org'

export const DUMMY_ORGS = [
  {id: DEFAULT_ORG_ID, name: DEFAULT_ORG_NAME},
  {name: NO_ORG},
  {id: '1', name: 'Red Team'},
  {id: '2', name: 'Blue Team'},
  {id: '3', name: 'Green Team'},
]

export const MOAR_DUMMY_ORGS = [
  {id: '128', name: 'Marketing'},
  {id: '127', name: 'Exec Team'},
  {id: '126', name: 'Lion Tamers'},
  {id: '125', name: 'Susl0rds'},
]
