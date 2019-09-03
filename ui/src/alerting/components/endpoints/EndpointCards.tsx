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
      text="Want to send notifications to Slack, LINEBREAK PagerDuty or an HTTP server? LINEBREAK LINEBREAK Try creating a Notification  Endpoint"
      highlightWords={['Notification', 'Endpoint']}
    />
  </EmptyState>
)

export default EndpointCards
