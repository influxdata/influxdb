import React, {PropTypes} from 'react'

import SlideToggle from 'shared/components/SlideToggle'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

const UsersTableSuperAdminCell = ({user, superAdmin, onChangeSuperAdmin}) => {
  const {colSuperAdmin} = USERS_TABLE

  return (
    <td style={{width: colSuperAdmin}} className="text-center">
      <SlideToggle
        active={superAdmin}
        onToggle={onChangeSuperAdmin(user, superAdmin)}
        size="xs"
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
