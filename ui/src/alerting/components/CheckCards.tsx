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
    <EmptyState size={ComponentSize.ExtraSmall} className="alert-column--empty">
      <EmptyState.Text
        text="Looks like you don’t have any Checks , why not create one?"
        highlightWords={['Checks']}
      />
    </EmptyState>
  )
}

export default CheckCards
