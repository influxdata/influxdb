// Libraries
import React, {FC} from 'react'

// Components
import EndpointCard from 'src/notifications/endpoints/components/EndpointCard'
import {EmptyState, ResourceList, ComponentSize} from '@influxdata/clockface'
import FilterList from 'src/shared/components/FilterList'

// Types
import {NotificationEndpoint} from 'src/types'

interface Props {
  endpoints: NotificationEndpoint[]
  searchTerm: string
}

const FilterEndpoints = FilterList<NotificationEndpoint>()

const EndpointCards: FC<Props> = ({endpoints, searchTerm}) => {
  const cards = endpoints =>
    endpoints.map(endpoint => (
      <EndpointCard key={endpoint.id} endpoint={endpoint} />
    ))

  const body = (
    <FilterEndpoints
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
    </FilterEndpoints>
  )

  return <ResourceList>{body}</ResourceList>
}

const EmptyEndpointList: FC<{searchTerm: string}> = ({searchTerm}) => {
  if (searchTerm) {
    return (
      <EmptyState size={ComponentSize.Small} className="alert-column--empty">
        <EmptyState.Text>
          "No <b>endpoints</b> match your search
        </EmptyState.Text>
      </EmptyState>
    )
  }

  return (
    <EmptyState size={ComponentSize.Small} className="alert-column--empty">
      <EmptyState.Text>
        Want to send notifications to Slack,
        <br />
        PagerDuty, Telegram or an HTTP server?
        <br />
        <br />
        Try creating a <b>Notification Endpoint</b>
      </EmptyState.Text>
    </EmptyState>
  )
}

export default EndpointCards
