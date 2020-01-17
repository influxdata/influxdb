import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import GetResource from 'src/resources/components/GetResource'
import DashboardPage from 'src/dashboards/components/DashboardPage'

// Types
import {ResourceType} from 'src/types'

type Props = WithRouterProps

const DashboardContainer: FC<Props> = ({params}) => {
  const {dashboardID, orgID} = params
  return (
    <GetResource resources={[{type: ResourceType.Dashboards, id: dashboardID}]}>
      <DashboardPage dashboardID={dashboardID} orgID={orgID} />
    </GetResource>
  )
}

export default withRouter<Props>(DashboardContainer)
