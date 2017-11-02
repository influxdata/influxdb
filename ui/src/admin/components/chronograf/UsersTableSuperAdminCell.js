import React, {PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

const UsersTableSuperAdminCell = ({user, superAdmin, onChangeSuperAdmin}) => {
  const {colSuperAdmin} = USERS_TABLE

  const items = [{text: 'True'}, {text: 'False'}]
  const selected =
    String(superAdmin).charAt(0).toUpperCase() + String(superAdmin).slice(1)

  return (
    <td style={{width: colSuperAdmin}}>
      <Dropdown
        items={items}
        selected={selected}
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
