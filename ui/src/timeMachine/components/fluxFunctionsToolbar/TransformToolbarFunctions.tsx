// Libraries
import React, {SFC, ReactElement} from 'react'
import {groupBy} from 'lodash'

// Components
import {EmptyState, ComponentSize} from '@influxdata/clockface'

// Types
import {FluxToolbarFunction} from 'src/types/shared'

interface Props {
  funcs: FluxToolbarFunction[]
  searchTerm?: string
  children: (funcs: {
    [category: string]: FluxToolbarFunction[]
  }) => JSX.Element | JSX.Element[]
}

const TransformToolbarFunctions: SFC<Props> = props => {
  const {searchTerm, funcs, children} = props

  const filteredFunctions = funcs.filter(func =>
    func.name.toLowerCase().includes(searchTerm.toLowerCase())
  )

  const groupedFunctions = groupBy(filteredFunctions, 'category')

  if (filteredFunctions.length === 0) {
    return (
      <EmptyState size={ComponentSize.ExtraSmall}>
        <EmptyState.Text>No functions match your search</EmptyState.Text>
      </EmptyState>
    )
  }

  return children(groupedFunctions) as ReactElement<any>
}

export default TransformToolbarFunctions
