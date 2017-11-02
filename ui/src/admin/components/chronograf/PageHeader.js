import React, {PropTypes} from 'react'

import SourceIndicator from 'shared/components/SourceIndicator'
import Authorized, {ADMIN_ROLE, SUPERADMIN_ROLE} from 'src/auth/Authorized'

const PageHeader = ({onShowManageOrgsOverlay, onShowCreateUserOverlay}) =>
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title">Chronograf Admin</h1>
      </div>
      <div className="page-header__right">
        <SourceIndicator />
        <Authorized requiredRole={ADMIN_ROLE}>
          <button
            className="btn btn-primary btn-sm"
            onClick={onShowCreateUserOverlay}
          >
            <span className="icon plus" />
            Create User
          </button>
        </Authorized>
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

const {func} = PropTypes

PageHeader.propTypes = {
  onShowManageOrgsOverlay: func.isRequired,
  onShowCreateUserOverlay: func.isRequired,
}

export default PageHeader
