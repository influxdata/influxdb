import _ from 'lodash'

import {SUPERADMIN_ROLE, MEMBER_ROLE} from 'src/auth/Authorized'

export const getMeRole = me => {
  return me.superAdmin
    ? SUPERADMIN_ROLE
    : _.get(_.first(_.get(me, 'roles', [])), 'name', MEMBER_ROLE)
}
