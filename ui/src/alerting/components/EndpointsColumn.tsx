// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import EndpointCards from 'src/alerting/components/endpoints/EndpointCards'
import AlertsColumn from 'src/alerting/components/AlertsColumn'
import {AppState} from 'src/types'

interface StateProps {
  endpoints: AppState['endpoints']['list']
}
type OwnProps = {}
type Props = OwnProps & WithRouterProps & StateProps

const EndpointsColumn: FC<Props> = ({router, params, endpoints}) => {
  const handleOpenOverlay = () => {
    const newRuleRoute = `/orgs/${params.orgID}/alerting/endpoints/new`
    router.push(newRuleRoute)
  }

  return (
    <AlertsColumn
      title="Notification Endpoints"
      testID="create-endpoint"
      onCreate={handleOpenOverlay}
    >
      <EndpointCards endpoints={endpoints} />
    </AlertsColumn>
  )
}

const mstp = ({endpoints}: AppState) => {
  return {endpoints: endpoints.list}
}

export default connect<StateProps>(mstp)(withRouter<OwnProps>(EndpointsColumn))
