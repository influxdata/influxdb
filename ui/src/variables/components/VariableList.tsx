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

type SortKey = keyof Variable | 'arguments.type'

interface Props {
  variables: Variable[]
  emptyState: JSX.Element
  onDeleteVariable: (variable: Variable) => void
  onFilterChange: (searchTerm: string) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
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
    const {emptyState, sortKey, sortDirection, onClickColumn} = this.props

    return (
      <>
        <ResourceList>
          <ResourceList.Header>
            <ResourceList.Sorter
              name="Name"
              sortKey={this.headerKeys[0]}
              sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
              onClick={onClickColumn}
            />
            <ResourceList.Sorter
              name="Type"
              sortKey={this.headerKeys[1]}
              sort={sortKey === this.headerKeys[1] ? sortDirection : Sort.None}
              onClick={onClickColumn}
            />
          </ResourceList.Header>
          <ResourceList.Body emptyState={emptyState}>
            {this.rows}
          </ResourceList.Body>
        </ResourceList>
      </>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name', 'arguments.type']
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
