// Libraries
import React, {FC} from 'react'

// Components
import EndpointCard from 'src/alerting/components/endpoints/EndpointCard'
import {EmptyState, ResourceList, ComponentSize} from '@influxdata/clockface'

// Types
import {NotificationEndpoint} from 'src/types'

interface Props {
  endpoints: NotificationEndpoint[]
}

const EndpointCards: FC<Props> = ({endpoints}) => {
  const cards = endpoints.map(endpoint => (
    <EndpointCard key={endpoint.id} endpoint={endpoint} />
  ))

  return (
    <ResourceList>
      <ResourceList.Body emptyState={<EmptyEndpointList />}>
        {cards}
      </ResourceList.Body>
    </ResourceList>
  )
}

const EmptyEndpointList: FC = () => (
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
)

export default EndpointCards
