import React, {Component, PropTypes} from 'react'

import Organization from 'src/admin/components/chronograf/Organization'
import NewOrganization from 'src/admin/components/chronograf/NewOrganization'
import DefaultOrganization from 'src/admin/components/chronograf/DefaultOrganization'

import {DEFAULT_ORG_ID} from 'src/admin/constants/dummyUsers'

class ManageOrgsOverlay extends Component {
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
    const {organizations, onDismiss, onDeleteOrg, onRenameOrg} = this.props
    const {isAddingOrganization} = this.state

    return (
      <div className="overlay-technology">
        <div className="manage-orgs-form">
          <div className="manage-orgs-form--header">
            <div className="page-header__left">
              <h1 className="page-header__title">Manage Organizations</h1>
            </div>
            <div className="page-header__right">
              <button
                className="btn btn-sm btn-primary"
                onClick={this.handleClickCreateOrganization}
                disabled={isAddingOrganization}
              >
                <span className="icon plus" /> Create Organization
              </button>
              <span className="page-header__dismiss" onClick={onDismiss} />
            </div>
          </div>
          <div className="manage-orgs-form--body">
            <div className="manage-orgs-form--org-labels">
              <div className="manage-orgs-form--id">ID</div>
              <div className="manage-orgs-form--name">Name</div>
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
                  ? <DefaultOrganization key={org.name} organization={org} />
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
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

ManageOrgsOverlay.propTypes = {
  organizations: arrayOf(
    shape({
      id: string, // when optimistically created, organization will not have an id
      name: string.isRequired,
    })
  ).isRequired,
  onDismiss: func.isRequired,
  onCreateOrg: func.isRequired,
  onDeleteOrg: func.isRequired,
  onRenameOrg: func.isRequired,
}
export default ManageOrgsOverlay
