import React from 'react'
import {
  ComponentColor,
  ComponentSize,
  EmptyState,
  Button,
  IconFont,
} from '@influxdata/clockface'

const FlowsIndexEmpty = () => {
  const handleClickCreate = () => {
    console.log('TODO: create flow')
  }

  return (
    <EmptyState size={ComponentSize.Large}>
      <EmptyState.Text>
        Looks like there aren't any <b>Flows</b>, why not create one?
      </EmptyState.Text>
      <Button
        icon={IconFont.Plus}
        color={ComponentColor.Primary}
        text="Create Flow"
        titleText="Click to create a Flow"
        onClick={handleClickCreate}
        testID="Create Flow"
      />
    </EmptyState>
  )
}

export default FlowsIndexEmpty
