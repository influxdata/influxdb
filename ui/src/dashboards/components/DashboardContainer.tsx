import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import GetResource from 'src/resources/components/GetResource'
import DashboardPage from 'src/dashboards/components/DashboardPage'
import GetTimeRange from 'src/dashboards/components/GetTimeRange'

// Types
import {ResourceType} from 'src/types'

type Props = WithRouterProps

const DashboardContainer: FC<Props> = ({params, children}) => {
  const {dashboardID, orgID} = params
  return (
    <GetResource resources={[{type: ResourceType.Dashboards, id: dashboardID}]}>
      <GetTimeRange />
      <DashboardPage dashboardID={dashboardID} orgID={orgID} />
      {children}
    </GetResource>
  )
}

export default withRouter<Props>(DashboardContainer)
