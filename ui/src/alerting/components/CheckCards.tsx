//Libraries
import React, {FunctionComponent} from 'react'

//Components
import CheckCard from 'src/alerting/components/CheckCard'
import {EmptyState, ResourceList} from '@influxdata/clockface'

// Types
import {Check} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  checks: Check[]
}

const CheckCards: FunctionComponent<Props> = ({checks}) => {
  return (
    <>
      <ResourceList>
        <ResourceList.Body emptyState={<EmptyChecksList />}>
          {checks.map(check => (
            <CheckCard key={check.id} check={check} />
          ))}
        </ResourceList.Body>
      </ResourceList>
    </>
  )
}

const EmptyChecksList: FunctionComponent = () => {
  return (
    <EmptyState size={ComponentSize.Small} className="alert-column--empty">
      <EmptyState.Text
        text="A Check  is a periodic query that the system performs against your time series data that will generate a status"
        highlightWords={['Check']}
      />
      <br />
      <a href="#" target="_blank">
        Documentation
      </a>
    </EmptyState>
  )
}

export default CheckCards
