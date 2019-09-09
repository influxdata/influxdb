//Libraries
import React, {FunctionComponent} from 'react'

//Components
import CheckCard from 'src/alerting/components/CheckCard'
import FilterList from 'src/shared/components/Filter'
import {
  EmptyState,
  ResourceList,
  Panel,
  Gradients,
  Button,
  IconFont,
  ComponentColor,
} from '@influxdata/clockface'

// Types
import {Check} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  checks: Check[]
  searchTerm: string
  showFirstTimeWidget: boolean
  onCreateCheck: () => void
}

const CheckCards: FunctionComponent<Props> = ({
  checks,
  searchTerm,
  showFirstTimeWidget,
  onCreateCheck,
}) => {
  const cards = cs => cs.map(c => <CheckCard key={c.id} check={c} />)
  const body = filtered => (
    <ResourceList.Body
      emptyState={
        <EmptyChecksList
          showFirstTimeWidget={showFirstTimeWidget}
          onCreateCheck={onCreateCheck}
          searchTerm={searchTerm}
        />
      }
    >
      {cards(filtered)}
    </ResourceList.Body>
  )
  const filteredChecks = (
    <FilterList<Check>
      list={checks}
      searchKeys={['name']}
      searchTerm={searchTerm}
    >
      {filtered => body(filtered)}
    </FilterList>
  )

  return (
    <>
      <ResourceList>{filteredChecks}</ResourceList>
    </>
  )
}

interface EmptyProps {
  showFirstTimeWidget: boolean
  onCreateCheck: () => void
  searchTerm: string
}

const EmptyChecksList: FunctionComponent<EmptyProps> = ({
  showFirstTimeWidget,
  onCreateCheck,
  searchTerm,
}) => {
  if (searchTerm) {
    return (
      <EmptyState size={ComponentSize.Small} className="alert-column--empty">
        <EmptyState.Text
          text="No checks  match your search"
          highlightWords={['checks']}
        />
      </EmptyState>
    )
  }

  if (showFirstTimeWidget) {
    return (
      <Panel
        gradient={Gradients.PolarExpress}
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
            To get started try creating a Check:
          </h5>
          <Button
            size={ComponentSize.Medium}
            color={ComponentColor.Primary}
            onClick={onCreateCheck}
            text="Create a Check"
            icon={IconFont.Plus}
          />
        </Panel.Body>
      </Panel>
    )
  }

  return (
    <EmptyState size={ComponentSize.Small} className="alert-column--empty">
      <EmptyState.Text
        text="Looks like you have not created a Check  yet LINEBREAK LINEBREAK You will need one to be notified about LINEBREAK any changes in system status"
        highlightWords={['Check']}
      />
    </EmptyState>
  )
}

export default CheckCards
