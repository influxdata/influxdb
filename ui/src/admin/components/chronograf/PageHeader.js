import React, {PropTypes} from 'react'

import SourceIndicator from 'shared/components/SourceIndicator'
import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

const PageHeader = ({onShowCreateOrgOverlay}) =>
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title">Chronograf Admin</h1>
      </div>
      <div className="page-header__right">
        <SourceIndicator />
        <Authorized requiredRole={SUPERADMIN_ROLE}>
          <button
            className="btn btn-primary btn-sm"
            onClick={onShowCreateOrgOverlay}
          >
            <span className="icon plus" />
            Create Organization
          </button>
        </Authorized>
      </div>
    </div>
  </div>

const {func} = PropTypes

PageHeader.propTypes = {
  onShowCreateOrgOverlay: func.isRequired,
}

export default PageHeader
