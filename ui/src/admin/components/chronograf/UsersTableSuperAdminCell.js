import React, {PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

import {SUPERADMIN_OPTION_ITEMS} from 'src/admin/constants/dummyUsers'
import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

const UsersTableSuperAdminCell = ({user, superAdmin, onChangeSuperAdmin}) => {
  const {colSuperAdmin} = USERS_TABLE

  return (
    <td style={{width: colSuperAdmin}}>
      <Dropdown
        items={SUPERADMIN_OPTION_ITEMS}
        selected={superAdmin ? 'yes' : 'no'}
        onChoose={onChangeSuperAdmin(user, superAdmin)}
        buttonColor="btn-primary"
        buttonSize="btn-xs"
        className="super-admin-toggle"
      />
    </td>
  )
}

const {bool, func, shape} = PropTypes

UsersTableSuperAdminCell.propTypes = {
  superAdmin: bool,
  user: shape(),
  onChangeSuperAdmin: func.isRequired,
}

export default UsersTableSuperAdminCell
