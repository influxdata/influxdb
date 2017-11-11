import React, {PropTypes, Component} from 'react'

import SlideToggle from 'shared/components/SlideToggle'

import {MEMBER_ROLE} from 'src/auth/Authorized'

// This is a non-editable organization row, used currently for DEFAULT_ORG
class OrganizationsTableRowDefault extends Component {
  toggleWhitelistOnly = () => {
    const {organization, onToggleWhitelistOnly} = this.props
    onToggleWhitelistOnly(organization)
  }

  render() {
    const {organization} = this.props

    return (
      <div className="orgs-table--org">
        <div className="orgs-table--id">
          {organization.id}
        </div>
        <div className="orgs-table--name-disabled">
          {organization.name}
        </div>
        <div className="orgs-table--whitelist">
          <SlideToggle
            size="xs"
            active={organization.whitelistOnly}
            onToggle={this.toggleWhitelistOnly}
          />
        </div>
        <div className="orgs-table--default-role-disabled">
          {MEMBER_ROLE}
        </div>
        <button
          className="btn btn-sm btn-default btn-square orgs-table--delete"
          disabled={true}
        >
          <span className="icon trash" />
        </button>
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

OrganizationsTableRowDefault.propTypes = {
  organization: shape({
    id: string,
    name: string.isRequired,
  }).isRequired,
  onToggleWhitelistOnly: func.isRequired,
}

export default OrganizationsTableRowDefault
