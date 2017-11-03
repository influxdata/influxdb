import React, {PropTypes} from 'react'

const DefaultOrganization = ({organization}) =>
  <div className="manage-orgs-form--org">
    <div className="manage-orgs-form--id">
      {organization.id}
    </div>
    <div className="manage-orgs-form--name-disabled">
      {organization.name}
    </div>
    <button
      className="btn btn-sm btn-default btn-square manage-orgs-form--delete"
      disabled={true}
    >
      <span className="icon trash" />
    </button>
  </div>

const {shape, string} = PropTypes

DefaultOrganization.propTypes = {
  organization: shape({
    id: string,
    name: string.isRequired,
  }).isRequired,
}

export default DefaultOrganization
