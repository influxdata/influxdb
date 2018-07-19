import React, {Component} from 'react'

import SlideToggle from 'src/reusable_ui/components/slide_toggle/SlideToggle'
import {ComponentColor, ComponentSize} from 'src/reusable_ui/types'

interface AuthConfig {
  superAdminNewUsers: boolean
}

interface Props {
  numUsers: number
  numOrganizations: number
  onClickCreateUser: () => void
  isCreatingUser: boolean
  onChangeAuthConfig: (fieldName: string) => (value: any) => void
  authConfig: AuthConfig
}

class AllUsersTableHeader extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    numUsers: 0,
    numOrganizations: 0,
    isCreatingUser: false,
  }

  public render() {
    const {
      numUsers,
      numOrganizations,
      onClickCreateUser,
      isCreatingUser,
      authConfig: {superAdminNewUsers},
    } = this.props

    const numUsersString = `${numUsers} User${numUsers === 1 ? '' : 's'}`
    const numOrganizationsString = `${numOrganizations} Org${
      numOrganizations === 1 ? '' : 's'
    }`

    return (
      <div className="panel-heading">
        <h2 className="panel-title">
          {numUsersString} across {numOrganizationsString}
        </h2>
        <div style={{display: 'flex', alignItems: 'center'}}>
          <div className="all-users-admin-toggle">
            <SlideToggle
              color={ComponentColor.Success}
              size={ComponentSize.Small}
              active={superAdminNewUsers}
              onChange={this.handleToggleClick}
            />
            <span>All new users are SuperAdmins</span>
          </div>
          <button
            className="btn btn-primary btn-sm"
            onClick={onClickCreateUser}
            disabled={isCreatingUser || !onClickCreateUser}
          >
            <span className="icon plus" />
            Add User
          </button>
        </div>
      </div>
    )
  }

  private handleToggleClick = (): void => {
    const {
      onChangeAuthConfig,
      authConfig: {superAdminNewUsers},
    } = this.props
    const fieldName = 'superAdminNewUsers'
    const newConfig = !superAdminNewUsers

    onChangeAuthConfig(fieldName)(newConfig)
  }
}

export default AllUsersTableHeader
