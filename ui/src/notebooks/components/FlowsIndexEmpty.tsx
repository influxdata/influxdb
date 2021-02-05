import React from 'react'
import {ComponentSize, EmptyState} from '@influxdata/clockface'
import FlowCreateButton from './FlowCreateButton'

const FlowsIndexEmpty = () => {
  return (
    <EmptyState size={ComponentSize.Large}>
      <EmptyState.Text>
        Looks like there aren't any <b>Flows</b>, why not create one?
      </EmptyState.Text>
      <FlowCreateButton />
    </EmptyState>
  )
}

export default FlowsIndexEmpty
