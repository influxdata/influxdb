import React, {PropTypes} from 'react'

import SourceIndicator from 'shared/components/SourceIndicator'

const DashboardsHeader = ({sourceName}) => {
  return (
    <div className="page-header">
      <div className="page-header__container">
        <div className="page-header__left">
          <h1 className="page-header__title">Dashboards</h1>
        </div>
        <div className="page-header__right">
          <SourceIndicator sourceName={sourceName} />
        </div>
      </div>
    </div>
  )
}

const {string} = PropTypes

DashboardsHeader.propTypes = {
  sourceName: string.isRequired,
}

export default DashboardsHeader
