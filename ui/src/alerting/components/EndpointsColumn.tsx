// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, IconFont, ComponentColor} from '@influxdata/clockface'
import EndpointCards from 'src/alerting/components/endpoints/EndpointCards'
import AlertsColumn from 'src/alerting/components/AlertsColumn'
import OverlayLink from 'src/overlays/components/OverlayLink'

// Types
import {AppState} from 'src/types'

interface StateProps {
  endpoints: AppState['endpoints']['list']
}

const EndpointsColumn: FC<StateProps> = ({endpoints}) => {
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
    <OverlayLink overlayID="create-endpoint">
      {onClick => (
        <Button
          color={ComponentColor.Secondary}
          text="Create"
          onClick={onClick}
          testID="create-endpoint"
          icon={IconFont.Plus}
        />
      )}
    </OverlayLink>
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

export default connect<StateProps>(mstp)(EndpointsColumn)
