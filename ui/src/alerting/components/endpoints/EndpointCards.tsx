// Libraries
import React, {FC} from 'react'

// Components
import EndpointCard from 'src/alerting/components/endpoints/EndpointCard'
import {EmptyState, ResourceList, ComponentSize} from '@influxdata/clockface'
import FilterList from 'src/shared/components/Filter'

// Types
import {NotificationEndpoint} from 'src/types'

interface Props {
  endpoints: NotificationEndpoint[]
  searchTerm: string
}

const EndpointCards: FC<Props> = ({endpoints, searchTerm}) => {
  const cards = endpoints =>
    endpoints.map(endpoint => (
      <EndpointCard key={endpoint.id} endpoint={endpoint} />
    ))

  const body = (
    <FilterList<NotificationEndpoint>
      list={endpoints}
      searchKeys={['name']}
      searchTerm={searchTerm}
    >
      {filteredEndpoints => (
        <ResourceList.Body
          emptyState={<EmptyEndpointList searchTerm={searchTerm} />}
        >
          {cards(filteredEndpoints)}
        </ResourceList.Body>
      )}
    </FilterList>
  )

  return <ResourceList>{body}</ResourceList>
}

const EmptyEndpointList: FC<{searchTerm: string}> = ({searchTerm}) => {
  if (searchTerm) {
    return (
      <EmptyState size={ComponentSize.Small} className="alert-column--empty">
        <EmptyState.Text
          text="No endpoints  match your search"
          highlightWords={['endpoints']}
        />
      </EmptyState>
    )
  }

  return (
    <EmptyState size={ComponentSize.Small} className="alert-column--empty">
      <EmptyState.Text
        text="Want to send notifications to Slack, LINEBREAK PagerDuty or an HTTP server? LINEBREAK LINEBREAK Try creating a Notification  Endpoint"
        highlightWords={['Notification', 'Endpoint']}
      />
    </EmptyState>
  )
}

export default EndpointCards
