import React, {Component, PropTypes} from 'react'

import uuid from 'node-uuid'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import OrganizationsTableRow from 'src/admin/components/chronograf/OrganizationsTableRow'
import OrganizationsTableRowNew from 'src/admin/components/chronograf/OrganizationsTableRowNew'
import QuestionMarkTooltip from 'shared/components/QuestionMarkTooltip'
import SlideToggle from 'shared/components/SlideToggle'

import {PUBLIC_TOOLTIP} from 'src/admin/constants/index'

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
      onTogglePublic,
      currentOrganization,
      authConfig: {superAdminNewUsers},
      onChangeAuthConfig,
    } = this.props
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
          <div className="fancytable--labels">
            <div className="fancytable--th orgs-table--active" />
            <div className="fancytable--th orgs-table--name">Name</div>
            <div className="fancytable--th orgs-table--public">
              Public{' '}
              <QuestionMarkTooltip tipID="public" tipContent={PUBLIC_TOOLTIP} />
            </div>
            <div className="fancytable--th orgs-table--default-role">
              Default Role
            </div>
            <div className="fancytable--th orgs-table--delete" />
          </div>
          {isCreatingOrganization
            ? <OrganizationsTableRowNew
                onCreateOrganization={this.handleCreateOrganization}
                onCancelCreateOrganization={this.handleCancelCreateOrganization}
              />
            : null}
          {organizations.map(org =>
            <OrganizationsTableRow
              key={uuid.v4()}
              organization={org}
              onTogglePublic={onTogglePublic}
              onDelete={onDeleteOrg}
              onRename={onRenameOrg}
              onChooseDefaultRole={onChooseDefaultRole}
              currentOrganization={currentOrganization}
            />
          )}
          <Authorized requiredRole={SUPERADMIN_ROLE}>
            <table className="table v-center superadmin-config">
              <thead>
                <tr>
                  <th style={{width: 70}}>Config</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td style={{width: 70}}>
                    <SlideToggle
                      size="xs"
                      active={superAdminNewUsers}
                      onToggle={onChangeAuthConfig('superAdminNewUsers')}
                    />
                  </td>
                  <td>All new users are SuperAdmins</td>
                </tr>
              </tbody>
            </table>
          </Authorized>
        </div>
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

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
  onTogglePublic: func.isRequired,
  onChooseDefaultRole: func.isRequired,
  onChangeAuthConfig: func.isRequired,
  authConfig: shape({
    superAdminNewUsers: bool,
  }),
}
export default OrganizationsTable
