import React, {Component, PropTypes} from 'react'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import Dropdown from 'shared/components/Dropdown'

class UsersTableHeader extends Component {
  constructor(props) {
    super(props)
  }

  handleChooseFilter = () => organization => {
    this.props.onFilterUsers({organization})
  }

  render() {
    const {organizations, organizationName} = this.props

    return (
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <Authorized
          requiredRole={SUPERADMIN_ROLE}
          replaceWith={
            <h2 className="panel-title">
              {organizationName} Users
            </h2>
          }
        >
          <div className="u-flex u-ai-center">
            <p className="dropdown-label">Filter Users</p>
            <Dropdown
              items={organizations.map(org => ({
                ...org,
                text: org.name,
              }))}
              selected={organizationName}
              onChoose={this.handleChooseFilter()}
              buttonSize="btn-md"
              className="dropdown-220"
            />
          </div>
        </Authorized>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

UsersTableHeader.propTypes = {
  organizationName: string.isRequired,
  organizations: arrayOf(shape),
  onFilterUsers: func.isRequired,
}

export default UsersTableHeader
