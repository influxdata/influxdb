import React, {Component, PropTypes} from 'react'

import uuid from 'node-uuid'

import OrganizationsTableRow from 'src/admin/components/chronograf/OrganizationsTableRow'
import OrganizationsTableRowDefault from 'src/admin/components/chronograf/OrganizationsTableRowDefault'
import OrganizationsTableRowNew from 'src/admin/components/chronograf/OrganizationsTableRowNew'

import {DEFAULT_ORG_ID} from 'src/admin/constants/dummyUsers'

class OrganizationsTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isCreatingOrganization: false,
    }
  }
  handleClickCreateOrganization = () => {
    this.setState({isCreatingOrganization: true})
  }

  handleCancelCreateOrganization = () => {
    this.setState({isCreatingOrganization: false})
  }

  handleCreateOrganization = newOrganization => {
    const {onCreateOrg} = this.props
    onCreateOrg(newOrganization)
    this.setState({isCreatingOrganization: false})
  }

  render() {
    const {organizations, onDeleteOrg, onRenameOrg} = this.props
    const {isCreatingOrganization} = this.state

    const tableTitle = `${organizations.length} Organization${organizations.length ===
    1
      ? ''
      : 's'}`

    return (
      <div className="panel panel-default">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">
            {tableTitle}
          </h2>
          <button
            className="btn btn-sm btn-primary"
            onClick={this.handleClickCreateOrganization}
            disabled={isCreatingOrganization}
          >
            <span className="icon plus" /> Create Organization
          </button>
        </div>
        <div className="panel-body">
          <div className="orgs-table--org-labels">
            <div className="orgs-table--id">ID</div>
            <div className="orgs-table--name">Name</div>
            <div className="orgs-table--default-role">Default Role</div>
            <div className="orgs-table--delete" />
          </div>
          {isCreatingOrganization
            ? <OrganizationsTableRowNew
                onCreateOrganization={this.handleCreateOrganization}
                onCancelCreateOrganization={this.handleCancelCreateOrganization}
              />
            : null}
          {organizations.map(
            org =>
              org.id === DEFAULT_ORG_ID
                ? <OrganizationsTableRowDefault
                    key={uuid.v4()}
                    organization={org}
                  />
                : <OrganizationsTableRow
                    key={uuid.v4()}
                    organization={org}
                    onDelete={onDeleteOrg}
                    onRename={onRenameOrg}
                  />
          )}
        </div>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

OrganizationsTable.propTypes = {
  organizations: arrayOf(
    shape({
      id: string, // when optimistically created, organization will not have an id
      name: string.isRequired,
    })
  ).isRequired,
  onCreateOrg: func.isRequired,
  onDeleteOrg: func.isRequired,
  onRenameOrg: func.isRequired,
}
export default OrganizationsTable
