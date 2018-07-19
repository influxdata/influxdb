import React, {Component} from 'react'
import PropTypes from 'prop-types'

import uuid from 'uuid'
import _ from 'lodash'

import OrganizationsTableRow from 'src/admin/components/chronograf/OrganizationsTableRow'
import OrganizationsTableRowNew from 'src/admin/components/chronograf/OrganizationsTableRowNew'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class OrganizationsTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isCreatingOrganization: false,
    }
  }

  shouldComponentUpdate(nextProps, nextState) {
    return (
      !_.isEqual(this.props, nextProps) || !_.isEqual(this.state, nextState)
    )
  }

  handleClickCreateOrganization = () => {
    this.setState({isCreatingOrganization: true})
  }

  handleCancelCreateOrganization = () => {
    this.setState({isCreatingOrganization: false})
  }

  handleCreateOrganization = organization => {
    const {onCreateOrg} = this.props
    onCreateOrg(organization)
    this.setState({isCreatingOrganization: false})
  }

  render() {
    const {
      organizations,
      onDeleteOrg,
      onRenameOrg,
      onChooseDefaultRole,
      currentOrganization,
    } = this.props
    const {isCreatingOrganization} = this.state

    const tableTitle = `${organizations.length} Organization${
      organizations.length === 1 ? '' : 's'
    }`

    if (!organizations.length) {
      return (
        <div className="panel panel-solid">
          <div className="panel-body">
            <div className="page-spinner" />
          </div>
        </div>
      )
    }
    return (
      <div className="panel panel-solid">
        <div className="panel-heading">
          <h2 className="panel-title">{tableTitle}</h2>
          <button
            className="btn btn-sm btn-primary"
            onClick={this.handleClickCreateOrganization}
            disabled={isCreatingOrganization}
          >
            <span className="icon plus" /> Create Organization
          </button>
        </div>
        <div className="panel-body">
          <div className="fancytable--labels">
            <div className="fancytable--th orgs-table--active" />
            <div className="fancytable--th orgs-table--name">Name</div>
            <div className="fancytable--th orgs-table--default-role">
              Default Role
            </div>
            <div className="fancytable--th orgs-table--delete" />
          </div>
          {isCreatingOrganization ? (
            <OrganizationsTableRowNew
              onCreateOrganization={this.handleCreateOrganization}
              onCancelCreateOrganization={this.handleCancelCreateOrganization}
            />
          ) : null}
          {organizations.map(org => (
            <OrganizationsTableRow
              key={uuid.v4()}
              organization={org}
              onDelete={onDeleteOrg}
              onRename={onRenameOrg}
              onChooseDefaultRole={onChooseDefaultRole}
              currentOrganization={currentOrganization}
            />
          ))}
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
  currentOrganization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  onCreateOrg: func.isRequired,
  onDeleteOrg: func.isRequired,
  onRenameOrg: func.isRequired,
  onChooseDefaultRole: func.isRequired,
}
export default OrganizationsTable
