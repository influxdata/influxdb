import React, {Component, PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

class UsersTableHeader extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {onFilterUsers, organizations, organizationName} = this.props

    return (
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <div className="u-flex u-ai-center">
          <p className="dropdown-label">Filter Users</p>
          <Dropdown
            items={organizations.map(org => ({
              ...org,
              text: org.name,
            }))}
            selected={organizationName}
            onChoose={onFilterUsers}
            buttonSize="btn-md"
            className="dropdown-220"
          />
        </div>
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
