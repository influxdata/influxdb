// Libraries
import React, {FC} from 'react'
import {EmptyState, ComponentSize} from '@influxdata/clockface'

interface Props {
  searchTerm: string
}

const TokensEmptyState: FC<Props> = ({searchTerm}) => {
  let emptyStateText =
    'There are not any Tokens associated with this account. Contact your administrator'

  if (searchTerm) {
    emptyStateText = 'No Tokens match your search term'
  }

  return (
    <EmptyState size={ComponentSize.Large}>
      <EmptyState.Text>{emptyStateText}</EmptyState.Text>
    </EmptyState>
  )
}

export default TokensEmptyState
