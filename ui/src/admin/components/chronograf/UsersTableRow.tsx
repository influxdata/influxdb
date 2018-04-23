import React, {PureComponent} from 'react'

import Dropdown from 'src/shared/components/Dropdown'
import ConfirmButton from 'src/shared/components/ConfirmButton'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {USER_ROLES} from 'src/admin/constants/chronografAdmin'
import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'
import {User, Role} from 'src/types'

interface Organization {
  name: string
  id: string
}

interface DropdownRole {
  text: string
  name: string
}

interface Props {
  user: User
  organization: Organization
  onChangeUserRole: (User, Role) => void
  onDelete: (User) => void
  meID: string
}

@ErrorHandling
class UsersTableRow extends PureComponent<Props> {
  public render() {
    const {user, onChangeUserRole} = this.props
    const {colRole, colProvider, colScheme} = USERS_TABLE

    return (
      <tr className={'chronograf-admin-table--user'}>
        <td>
          {this.isMe ? (
            <strong className="chronograf-user--me">
              <span className="icon user" />
              {user.name}
            </strong>
          ) : (
            <strong>{user.name}</strong>
          )}
        </td>
        <td style={{width: colRole}}>
          <span className="chronograf-user--role">
            <Dropdown
              items={this.rolesDropdownItems}
              selected={this.currentRole.name}
              onChoose={onChangeUserRole(user, this.currentRole)}
              buttonColor="btn-primary"
              buttonSize="btn-xs"
              className="dropdown-stretch"
            />
          </span>
        </td>
        <td style={{width: colProvider}}>{user.provider}</td>
        <td style={{width: colScheme}}>{user.scheme}</td>
        <td className="text-right">
          <ConfirmButton
            confirmText={this.confirmationText}
            confirmAction={this.handleDelete}
            size="btn-xs"
            type="btn-danger"
            text="Remove"
            customClass="table--show-on-row-hover"
          />
        </td>
      </tr>
    )
  }

  private handleDelete = (): void => {
    const {user, onDelete} = this.props

    onDelete(user)
  }

  private get rolesDropdownItems(): DropdownRole[] {
    return USER_ROLES.map(r => ({
      ...r,
      text: r.name,
    }))
  }

  private get currentRole(): Role {
    const {user, organization} = this.props

    return user.roles.find(role => role.organization === organization.id)
  }

  private get isMe(): boolean {
    const {user, meID} = this.props

    return user.id === meID
  }

  private get confirmationText(): string {
    return 'Remove this user\nfrom Current Org?'
  }
}

export default UsersTableRow
