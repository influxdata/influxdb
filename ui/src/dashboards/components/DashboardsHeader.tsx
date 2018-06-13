import React from 'react'

import SourceIndicator from 'src/shared/components/SourceIndicator'

const DashboardsHeader = (): JSX.Element => (
  <div className="page-header">
    <div className="page-header--container">
      <div className="page-header--left">
        <h1 className="page-header--title">Dashboards</h1>
      </div>
      <div className="page-header--right">
        <SourceIndicator />
      </div>
    </div>
  </div>
)

export default DashboardsHeader
