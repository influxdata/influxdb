//Libraries
import React, {FunctionComponent} from 'react'

//Components
import CheckCard from 'src/checks/components/CheckCard'
import FilterList from 'src/shared/components/FilterList'
import {
  EmptyState,
  ResourceList,
  Panel,
  Gradients,
  Button,
  IconFont,
  ComponentColor,
  ButtonShape,
} from '@influxdata/clockface'

// Types
import {Check} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'

const FilterChecks = FilterList<Check>()

interface Props {
  checks: Check[]
  searchTerm: string
  showFirstTimeWidget: boolean
  onCreateThreshold: () => void
  onCreateDeadman: () => void
}

const CheckCards: FunctionComponent<Props> = ({
  checks,
  searchTerm,
  showFirstTimeWidget,
  onCreateThreshold,
  onCreateDeadman,
}) => {
  const cards = cs => cs.map(c => <CheckCard key={c.id} check={c} />)
  const body = filtered => (
    <ResourceList.Body
      emptyState={
        <EmptyChecksList
          showFirstTimeWidget={showFirstTimeWidget}
          onCreateThreshold={onCreateThreshold}
          onCreateDeadman={onCreateDeadman}
          searchTerm={searchTerm}
        />
      }
    >
      {cards(filtered)}
    </ResourceList.Body>
  )
  const filteredChecks = (
    <FilterChecks list={checks} searchKeys={['name']} searchTerm={searchTerm}>
      {filtered => body(filtered)}
    </FilterChecks>
  )

  return (
    <>
      <ResourceList>{filteredChecks}</ResourceList>
    </>
  )
}

interface EmptyProps {
  showFirstTimeWidget: boolean
  onCreateThreshold: () => void
  onCreateDeadman: () => void
  searchTerm: string
}

const EmptyChecksList: FunctionComponent<EmptyProps> = ({
  showFirstTimeWidget,
  onCreateThreshold,
  onCreateDeadman,
  searchTerm,
}) => {
  if (searchTerm) {
    return (
      <EmptyState size={ComponentSize.Small} className="alert-column--empty">
        <EmptyState.Text>
          No <b>checks</b> match your search
        </EmptyState.Text>
      </EmptyState>
    )
  }

  if (showFirstTimeWidget) {
    return (
      <Panel gradient={Gradients.PolarExpress} className="alerting-first-time">
        <Panel.Body size={ComponentSize.Large}>
          <h1>Get started monitoring by creating a check</h1>
          <h5>When a value crosses a specific threshold:</h5>
          <Button
            size={ComponentSize.Medium}
            color={ComponentColor.Primary}
            onClick={onCreateThreshold}
            text="Threshold Check"
            icon={IconFont.Plus}
            shape={ButtonShape.StretchToFit}
          />
          <h5>If a service stops sending metrics:</h5>
          <Button
            size={ComponentSize.Medium}
            color={ComponentColor.Primary}
            onClick={onCreateDeadman}
            text="Deadman Check"
            icon={IconFont.Plus}
            shape={ButtonShape.StretchToFit}
          />
        </Panel.Body>
      </Panel>
    )
  }

  return (
    <EmptyState size={ComponentSize.Small} className="alert-column--empty">
      <EmptyState.Text>
        Looks like you have not created a <b>Check</b> yet
        <br />
        <br />
        You will need one to be notified about
        <br /> any changes in system status
      </EmptyState.Text>
    </EmptyState>
  )
}

export default CheckCards
