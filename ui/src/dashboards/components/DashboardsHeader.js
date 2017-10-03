import React from 'react'

import SourceIndicator from 'shared/components/SourceIndicator'

const DashboardsHeader = () =>
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title">Dashboards</h1>
      </div>
      <div className="page-header__right">
        <SourceIndicator />
      </div>
    </div>
  </div>

export default DashboardsHeader
