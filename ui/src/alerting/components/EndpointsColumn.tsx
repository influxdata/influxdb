// Libraries
import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {EmptyState, ComponentSize} from '@influxdata/clockface'
import AlertsColumn from 'src/alerting/components/AlertsColumn'

type OwnProps = {}
type Props = OwnProps & WithRouterProps

const EndpointsColumn: FC<Props> = ({router, params}) => {
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
      <EmptyState size={ComponentSize.Small} className="alert-column--empty">
        <EmptyState.Text
          text="A Notification  Endpoint  stores the information to connect to a third party service that can receive notifications like Slack, PagerDuty, or an HTTP server"
          highlightWords={['Notification', 'Endpoint']}
        />
        <br />
        <a href="#" target="_blank">
          Documentation
        </a>
      </EmptyState>
    </AlertsColumn>
  )
}

export default withRouter<OwnProps>(EndpointsColumn)
