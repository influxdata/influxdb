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

export const DEFAULT_ORG_ID = 'default'
