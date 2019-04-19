// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'

// Components
import {IndexList, Overlay} from 'src/clockface'
import VariableRow from 'src/variables/components/VariableRow'
import UpdateVariableOverlay from 'src/variables/components/UpdateVariableOverlay'

// Types
import {IVariable as Variable} from '@influxdata/influx'
import {OverlayState} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'
import {Sort} from '@influxdata/clockface'

// Selectors
import {getSortedResources} from 'src/shared/utils/sort'

type SortKey = keyof Variable

interface Props {
  variables: Variable[]
  emptyState: JSX.Element
  onDeleteVariable: (variable: Variable) => void
  onUpdateVariable: (variable: Variable) => void
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
    const {
      emptyState,
      variables,
      sortKey,
      sortDirection,
      onClickColumn,
    } = this.props

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell
              sortKey={this.headerKeys[0]}
              sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
              columnName="Name"
              width="60%"
              onClick={onClickColumn}
            />
            <IndexList.HeaderCell columnName="Type" width="40%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={emptyState}>
            {this.rows}
          </IndexList.Body>
        </IndexList>
        <Overlay visible={this.isVariableOverlayVisible}>
          <UpdateVariableOverlay
            variable={this.variable}
            variables={variables}
            onCloseOverlay={this.handleCloseOverlay}
            onUpdateVariable={this.handleUpdateVariable}
          />
        </Overlay>
      </>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name']
  }

  private get rows(): JSX.Element[] {
    const {
      variables,
      sortKey,
      sortDirection,
      sortType,
      onDeleteVariable,
      onUpdateVariable,
      onFilterChange,
    } = this.props
    const sortedVariables = this.memGetSortedResources(
      variables,
      sortKey,
      sortDirection,
      sortType
    )

    return sortedVariables.map((variable, index) => (
      <VariableRow
        key={variable.id || `variable-${index}`}
        variable={variable}
        onDeleteVariable={onDeleteVariable}
        onUpdateVariableName={onUpdateVariable}
        onEditVariable={this.handleStartEdit}
        onFilterChange={onFilterChange}
      />
    ))
  }

  private get variable(): Variable {
    return this.props.variables.find(v => v.id === this.state.variableID)
  }

  private get isVariableOverlayVisible(): boolean {
    return this.state.variableOverlayState === OverlayState.Open
  }

  private handleCloseOverlay = () => {
    this.setState({variableOverlayState: OverlayState.Closed, variableID: null})
  }

  private handleStartEdit = (variable: Variable) => {
    this.setState({
      variableID: variable.id,
      variableOverlayState: OverlayState.Open,
    })
  }

  private handleUpdateVariable = async (variable: Variable): Promise<void> => {
    this.props.onUpdateVariable(variable)
  }
}
