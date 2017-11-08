import React, {PropTypes} from 'react'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

const PageHeader = ({onShowManageOrgsOverlay, currentOrganization}) =>
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title">
          {currentOrganization.name}
        </h1>
      </div>
      <div className="page-header__right">
        <Authorized requiredRole={SUPERADMIN_ROLE}>
          <button
            className="btn btn-primary btn-sm"
            onClick={onShowManageOrgsOverlay}
          >
            <span className="icon cog-thick" />
            Manage Organizations
          </button>
        </Authorized>
      </div>
    </div>
  </div>

const {func, shape, string} = PropTypes

PageHeader.propTypes = {
  onShowManageOrgsOverlay: func.isRequired,
  currentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
}

export default PageHeader
