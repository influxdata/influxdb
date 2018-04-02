import React, {PureComponent} from 'react'
import classnames from 'classnames'

import _ from 'lodash'

import MultiSelectDropdown from 'src/shared/components/MultiSelectDropdown'

import {USERS_TABLE} from 'src/admin/constants/tableSizing'
import {User} from 'src/types/influxAdmin'

interface Props {
  user: User
  allRoles: any[]
  onUpdateRoles: (user: User, roles: any[]) => void
}

class UserRoleDropdown extends PureComponent<Props> {
  public render() {
    const {allRoles} = this.props

    return (
      <MultiSelectDropdown
        buttonSize="btn-xs"
        buttonColor="btn-primary"
        items={allRoles}
        label={this.rolesLabel}
        selectedItems={this.roles}
        customClass={this.rolesClass}
        onApply={this.handleUpdateRoles}
        resetStateOnReceiveProps={false}
      />
    )
  }

  private handleUpdateRoles = (roleNames): void => {
    const {user, allRoles, onUpdateRoles} = this.props
    const roles = allRoles.filter(r => roleNames.find(rn => rn.name === r.name))

    onUpdateRoles(user, roles)
  }

  private get roles() {
    const roles = _.get(this.props.user, 'roles', [])
    return roles.map(r => ({name: r.name}))
  }

  private get rolesClass() {
    return classnames(`dropdown-${USERS_TABLE.colRoles}`, {
      'admin-table--multi-select-empty': !this.roles.length,
    })
  }

  private get rolesLabel() {
    return this.roles.length ? '' : 'Select Roles'
  }
}

export default UserRoleDropdown
