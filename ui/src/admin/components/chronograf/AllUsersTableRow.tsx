import React, {PureComponent} from 'react'
import _ from 'lodash'

import Tags from 'src/shared/components/Tags'
import SlideToggle from 'src/shared/components/SlideToggle'
import ConfirmButton from 'src/shared/components/ConfirmButton'

import {ALL_USERS_TABLE} from 'src/admin/constants/chronografTableSizing'
import {ErrorHandling} from 'src/shared/decorators/errors'

const {
  colOrganizations,
  colProvider,
  colScheme,
  colSuperAdmin,
  colActions,
} = ALL_USERS_TABLE

interface Organization {
  id: string
  name: string
}

interface Role {
  organization: string
}

interface User {
  id: string
  name: string
  roles: Role[]
  superAdmin: boolean
  scheme: string
  provider: string
}

interface Props {
  user: User
  organization: Organization
  onAddToOrganization: (user: User) => () => void
  onRemoveFromOrganization: (user: User) => () => void
  onChangeSuperAdmin: (user: User) => () => void
  onDelete: (user: User) => void
  meID: string
  organizations: Organization[]
}

@ErrorHandling
export default class AllUsersTableRow extends PureComponent<Props> {
  public render() {
    const {
      user,
      onRemoveFromOrganization,
      onAddToOrganization,
      onChangeSuperAdmin,
    } = this.props

    return (
      <tr className={'chronograf-admin-table--user'}>
        {this.userNameTableCell}
        <td style={{width: colOrganizations}}>
          <Tags
            tags={this.userOrganizationTags}
            onDeleteTag={onRemoveFromOrganization(user)}
            addMenuItems={this.dropdownOrganizationsItems}
            addMenuChoose={onAddToOrganization(user)}
          />
        </td>
        <td style={{width: colProvider}}>{user.provider}</td>
        <td style={{width: colScheme}}>{user.scheme}</td>
        <td style={{width: colSuperAdmin}} className="text-center">
          <SlideToggle
            active={user.superAdmin}
            onToggle={onChangeSuperAdmin(user)}
            size="xs"
            disabled={this.userIsMe}
          />
        </td>
        <td style={{textAlign: 'right', width: colActions}}>
          <ConfirmButton
            confirmText={this.removeWarning}
            confirmAction={this.handleDelete}
            size="btn-xs"
            type="btn-danger"
            text="Delete"
            customClass="table--show-on-row-hover"
          />
        </td>
      </tr>
    )
  }

  private get userNameTableCell() {
    const {user} = this.props

    return (
      <td>
        {this.userIsMe ? (
          <strong className="chronograf-user--me">
            <span className="icon user" />
            {user.name}
          </strong>
        ) : (
          <strong>{user.name}</strong>
        )}
      </td>
    )
  }

  private get userOrganizationTags() {
    return this.userRoles.map((role: Role) => ({
      ...role,
      name: this.findOrganizationByRole(role).name,
    }))
  }

  private findOrganizationByRole(role: Role): Organization | null {
    const {organizations} = this.props

    return _.find(organizations, org => role.organization === org.id)
  }

  private get removeWarning() {
    if (this.userIsMe) {
      return 'Delete your user record\nand log yourself out?'
    }

    return 'Delete this user?'
  }

  private get userIsMe() {
    const {user, meID} = this.props
    return user.id === meID
  }

  private get dropdownOrganizationsItems() {
    return this.userOrganizations.map(o => ({...o, text: o.name}))
  }

  private get userOrganizations() {
    const {organizations} = this.props
    return _.filter(organizations, _.negate(this.isUserOrganization))
  }

  private get userRoles(): Role[] {
    return _.get(this.props.user, 'roles', [])
  }

  private isUserOrganization = (organization): boolean => {
    return !!_.find(
      this.userRoles,
      role => role.organization === organization.id
    )
  }

  private handleDelete = () => {
    const {onDelete, user} = this.props
    onDelete(user)
  }
}
