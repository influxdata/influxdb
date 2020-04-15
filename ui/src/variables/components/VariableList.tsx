// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'

// Components
import {ResourceList} from '@influxdata/clockface'
import VariableCard from 'src/variables/components/VariableCard'

// Types
import {OverlayState, Variable} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'
import {Sort} from '@influxdata/clockface'

// Selectors
import {getSortedResources} from 'src/shared/utils/sort'

interface Props {
  variables: Variable[]
  emptyState: JSX.Element
  onDeleteVariable: (variable: Variable) => void
  onFilterChange: (searchTerm: string) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
}

interface State {
  variableID: string
  variableOverlayState: OverlayState
  sortedVariables: Variable[]
}

export default class VariableList extends PureComponent<Props, State> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  constructor(props) {
    super(props)

    this.state = {
      variableID: null,
      variableOverlayState: OverlayState.Closed,
      sortedVariables: this.props.variables,
    }
  }

  public render() {
    const {emptyState} = this.props

    return (
      <>
        <ResourceList>
          <ResourceList.Body emptyState={emptyState}>
            {this.rows}
          </ResourceList.Body>
        </ResourceList>
      </>
    )
  }

  private get rows(): JSX.Element[] {
    const {
      variables,
      sortKey,
      sortDirection,
      sortType,
      onDeleteVariable,
      onFilterChange,
    } = this.props

    const sortedVariables = this.memGetSortedResources(
      variables,
      sortKey,
      sortDirection,
      sortType
    )

    return sortedVariables.map((variable, index) => (
      <VariableCard
        key={variable.id || `variable-${index}`}
        variable={variable}
        onDeleteVariable={onDeleteVariable}
        onEditVariable={this.handleStartEdit}
        onFilterChange={onFilterChange}
      />
    ))
  }

  private handleStartEdit = (variable: Variable) => {
    this.setState({
      variableID: variable.id,
      variableOverlayState: OverlayState.Open,
    })
  }
}
