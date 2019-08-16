// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {EmptyState, ComponentSize} from '@influxdata/clockface'
import AlertsColumn from 'src/alerting/components/AlertsColumn'

const EndpointsColumn: FunctionComponent = () => {
  return (
    <AlertsColumn
      title="Notification Endpoints"
      testID="create-endpoint"
      onCreate={() => {}}
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

export default EndpointsColumn
