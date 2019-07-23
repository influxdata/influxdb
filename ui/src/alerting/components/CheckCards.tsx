import React, {FunctionComponent} from 'react'

// Types
import {Check} from 'src/types'
import {EmptyState, ComponentSize} from '@influxdata/clockface'

interface Props {
  checks: Check[]
}

const CheckCards: FunctionComponent<Props> = ({checks}) => {
  if (checks && checks.length) {
    return <></>
  }
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
