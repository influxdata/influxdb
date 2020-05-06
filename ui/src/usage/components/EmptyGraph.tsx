import React, {FC} from 'react'

import {
  Panel,
  ComponentSize,
  InfluxColors,
  EmptyState,
  EmptyStateText,
} from '@influxdata/clockface'

interface Props {
  title: string
  isError: boolean
  errorMessage?: string
}

const getText = (isError: boolean, errorMessage: string) => {
  if (isError) {
    return errorMessage || 'Failed to load data'
  }

  return 'No data to display'
}

const EmptyGraph: FC<Props> = ({title, isError, errorMessage}) => {
  return (
    <Panel backgroundColor={InfluxColors.Onyx}>
      <Panel.Header size={ComponentSize.ExtraSmall}>
        <h5>{title}</h5>
      </Panel.Header>
      <Panel.Body size={ComponentSize.ExtraSmall}>
        <EmptyState>
          <EmptyStateText>{getText(isError, errorMessage)}</EmptyStateText>
        </EmptyState>
      </Panel.Body>
    </Panel>
  )
}

export default EmptyGraph
