//Libraries
import React, {FunctionComponent} from 'react'

//Components
import CheckCard from 'src/alerting/components/CheckCard'
import {EmptyState, ResourceList, Panel, Gradients} from '@influxdata/clockface'

// Types
import {Check} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  checks: Check[]
  showFirstTimeWidget: boolean
}

const CheckCards: FunctionComponent<Props> = ({
  checks,
  showFirstTimeWidget,
}) => {
  return (
    <>
      <ResourceList>
        <ResourceList.Body
          emptyState={
            <EmptyChecksList showFirstTimeWidget={showFirstTimeWidget} />
          }
        >
          {checks.map(check => (
            <CheckCard key={check.id} check={check} />
          ))}
        </ResourceList.Body>
      </ResourceList>
    </>
  )
}

interface EmptyProps {
  showFirstTimeWidget: boolean
}

const EmptyChecksList: FunctionComponent<EmptyProps> = ({
  showFirstTimeWidget,
}) => {
  if (showFirstTimeWidget) {
    return (
      <Panel
        gradient={Gradients.GundamPilot}
        size={ComponentSize.Large}
        className="alerting-first-time"
      >
        <Panel.Body>
          <h1>
            Looks like it's your
            <br />
            first time here
          </h1>
          <h5>
            Welcome to our new Monitoring & Alerting feature!
            <br />
            Try creating a <strong>Check</strong> to get started
          </h5>
        </Panel.Body>
      </Panel>
    )
  }

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
