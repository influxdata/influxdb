//Libraries
import React, {FunctionComponent} from 'react'

//Components
import CheckCard from 'src/alerting/components/CheckCard'
import {ResourceList} from 'src/clockface'

// Types
import {Check} from 'src/types'
import {EmptyState, ComponentSize} from '@influxdata/clockface'

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
    <EmptyState size={ComponentSize.ExtraSmall}>
      <EmptyState.Text
        text="Looks like you don’t have any Checks , why not create one?"
        highlightWords={['Checks']}
      />
    </EmptyState>
  )
}

export default CheckCards
