import React, {PropTypes} from 'react'

import SourceIndicator from 'shared/components/SourceIndicator'

const PageHeader = ({onShowCreateOrgOverlay}) =>
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title">Chronograf Admin</h1>
      </div>
      <div className="page-header__right">
        <SourceIndicator />
        <button
          className="btn btn-primary btn-sm"
          onClick={onShowCreateOrgOverlay}
        >
          <span className="icon plus" />
          Create Organization
        </button>
      </div>
    </div>
  </div>

const {func} = PropTypes

PageHeader.propTypes = {
  onShowCreateOrgOverlay: func.isRequired,
}

export default PageHeader
