import _ from 'lodash'

import {SUPERADMIN_ROLE, MEMBER_ROLE} from 'src/auth/Authorized'

export const getMeRole = me => {
  const currentRoleOrg = me.roles.find(
    role => me.currentOrganization.id === role.organization
  )
  const currentRole = _.get(currentRoleOrg, 'name', MEMBER_ROLE)

  return me.superAdmin ? SUPERADMIN_ROLE : currentRole
}

export const isSameUser = (userA, userB) => {
  return (
    userA.name === userB.name &&
    userA.provider === userB.provider &&
    userA.scheme === userB.scheme
  )
}
