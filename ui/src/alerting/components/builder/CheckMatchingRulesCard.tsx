// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {EmptyState, ComponentSize} from '@influxdata/clockface'

const CheckMatchingRulesCard: FunctionComponent = () => {
  return (
    <EmptyState
      size={ComponentSize.Small}
      className="alert-builder--card__empty"
    >
      <EmptyState.Text text="Notification Rules configured to act on tag sets matching this Alert Check will automatically show up here" />
      <EmptyState.Text text="Looks like no notification rules match the tag set defined in this Alert Check" />
    </EmptyState>
  )
}

export default CheckMatchingRulesCard
