import React, {Component, PropTypes} from 'react'

import Organization from 'src/admin/components/chronograf/Organization'
import DefaultOrganization from 'src/admin/components/chronograf/DefaultOrganization'
import NewOrganization from 'src/admin/components/chronograf/NewOrganization'

import {DEFAULT_ORG_ID} from 'src/admin/constants/dummyUsers'

class OrganizationsTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isAddingOrganization: false,
    }
  }
  handleClickCreateOrganization = () => {
    this.setState({isAddingOrganization: true})
  }

  handleCancelCreateOrganization = () => {
    this.setState({isAddingOrganization: false})
  }

  handleCreateOrganization = newOrganization => {
    const {onCreateOrg} = this.props
    onCreateOrg(newOrganization)
  }

  render() {
    const {organizations, onDeleteOrg, onRenameOrg} = this.props
    const {isAddingOrganization} = this.state

    const tableTitle =
      organizations.length === 1
        ? '1 Organizations'
        : `${organizations.length} Organizations`

    return (
      <div className="container-fluid">
        <div className="row">
          <div className="col-xs-12">
            <div className="panel panel-minimal">
              <div className="panel-heading u-flex u-ai-center u-jc-space-between">
                <h2 className="panel-title">
                  {tableTitle}
                </h2>
                <button
                  className="btn btn-sm btn-primary"
                  onClick={this.handleClickCreateOrganization}
                  disabled={isAddingOrganization}
                >
                  <span className="icon plus" /> Create Organization
                </button>
              </div>
              <div className="panel-body">
                <div className="orgs-table--org-labels">
                  <div className="orgs-table--id">ID</div>
                  <div className="orgs-table--name">Name</div>
                </div>
                {isAddingOrganization
                  ? <NewOrganization
                      onCreateOrganization={this.handleCreateOrganization}
                      onCancelCreateOrganization={
                        this.handleCancelCreateOrganization
                      }
                    />
                  : null}
                {organizations.map(
                  org =>
                    org.id === DEFAULT_ORG_ID
                      ? <DefaultOrganization
                          key={org.name}
                          organization={org}
                        />
                      : <Organization
                          key={org.name}
                          organization={org}
                          onDelete={onDeleteOrg}
                          onRename={onRenameOrg}
                        />
                )}
              </div>
            </div>
          </div>
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
