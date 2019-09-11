// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Button, IconFont, ComponentColor} from '@influxdata/clockface'
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

  const tooltipContents = (
    <>
      A <strong>Notification Endpoint</strong> stores the information to connect
      <br />
      to a third party service that can receive notifications
      <br />
      like Slack, PagerDuty, or an HTTP server
      <br />
      <br />
      <a
        href="https://v2.docs.influxdata.com/v2.0/monitor-alert/notification-endpoints/create"
        target="_blank"
      >
        Read Documentation
      </a>
    </>
  )

  const createButton = (
    <Button
      color={ComponentColor.Secondary}
      text="Create"
      onClick={handleOpenOverlay}
      testID="create-endpoint"
      icon={IconFont.Plus}
    />
  )

  return (
    <AlertsColumn
      title="Notification Endpoints"
      createButton={createButton}
      questionMarkTooltipContents={tooltipContents}
    >
      {searchTerm => (
        <EndpointCards endpoints={endpoints} searchTerm={searchTerm} />
      )}
    </AlertsColumn>
  )
}

const mstp = ({endpoints}: AppState) => {
  return {endpoints: endpoints.list}
}

export default connect<StateProps>(mstp)(withRouter<OwnProps>(EndpointsColumn))
