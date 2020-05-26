// Libraries
import React, {FC} from 'react'
import {EmptyState, ComponentSize} from '@influxdata/clockface'
import GenerateTokenDropdown from './GenerateTokenDropdown'

interface Props {
  searchTerm: string
}

const TokensEmptyState: FC<Props> = ({searchTerm}) => {
  if (searchTerm) {
    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text>No Tokens match your search</EmptyState.Text>
      </EmptyState>
    )
  }

  return (
    <EmptyState size={ComponentSize.Large}>
      <EmptyState.Text>
        Looks like you don't have any <b>Tokens</b>, why not add one?
      </EmptyState.Text>
      <GenerateTokenDropdown />
    </EmptyState>
  )
}

export default TokensEmptyState
