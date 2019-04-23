// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Utils
import {updateVariable, deleteVariable} from 'src/variables/actions'
import {extractVariablesList} from 'src/variables/selectors'

// Components
import {Input, EmptyState} from '@influxdata/clockface'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import VariableList from 'src/variables/components/VariableList'
import FilterList from 'src/shared/components/Filter'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'
import {Sort} from '@influxdata/clockface'

// Types
import {OverlayState} from 'src/types'
import {AppState} from 'src/types'
import {IVariable as Variable} from '@influxdata/influx'
import {IconFont, ComponentSize} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/utils/sort'

interface StateProps {
  variables: Variable[]
}

interface DispatchProps {
  onUpdateVariable: typeof updateVariable
  onDeleteVariable: typeof deleteVariable
}

type Props = StateProps & DispatchProps & WithRouterProps

interface State {
  searchTerm: string
  importOverlayState: OverlayState
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
}

type SortKey = keyof Variable

class VariablesTab extends PureComponent<Props, State> {
  public state: State = {
    searchTerm: '',
    importOverlayState: OverlayState.Closed,
    sortKey: 'name',
    sortDirection: Sort.Ascending,
    sortType: SortTypes.String,
  }

  public render() {
    const {variables} = this.props
    const {searchTerm, sortKey, sortDirection, sortType} = this.state

    return (
      <>
        <TabbedPageHeader>
          <Input
            icon={IconFont.Search}
            placeholder="Filter variables..."
            widthPixels={290}
            value={searchTerm}
            onChange={this.handleFilterChange}
            onBlur={this.handleFilterBlur}
          />
          <AddResourceDropdown
            resourceName="Variable"
            onSelectImport={this.handleOpenImportOverlay}
            onSelectNew={this.handleOpenCreateOverlay}
          />
        </TabbedPageHeader>
        <GetResources resource={ResourceTypes.Labels}>
          <FilterList<Variable>
            searchTerm={searchTerm}
            searchKeys={['name', 'labels[].name']}
            list={variables}
          >
            {variables => (
              <VariableList
                variables={variables}
                emptyState={this.emptyState}
                onDeleteVariable={this.handleDeleteVariable}
                onUpdateVariable={this.handleUpdateVariable}
                onFilterChange={this.handleFilterUpdate}
                sortKey={sortKey}
                sortDirection={sortDirection}
                sortType={sortType}
                onClickColumn={this.handleClickColumn}
              />
            )}
          </FilterList>
        </GetResources>
      </>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    const sortType = SortTypes.String
    this.setState({sortKey, sortDirection: nextSort, sortType})
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (!searchTerm) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text
            text={`Looks like there aren't any Variables, why not create one?`}
            highlightWords={['Variables']}
          />
          <AddResourceDropdown
            resourceName="Variable"
            onSelectImport={this.handleOpenImportOverlay}
            onSelectNew={this.handleOpenCreateOverlay}
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="No Variables match your query" />
      </EmptyState>
    )
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.handleFilterUpdate(e.target.value)
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterUpdate = (searchTerm: string) => {
    this.setState({searchTerm})
  }

  private handleOpenImportOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/variables/import`)
  }

  private handleOpenCreateOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/variables/new`)
  }

  private handleUpdateVariable = (variable: Partial<Variable>): void => {
    const {onUpdateVariable} = this.props

    onUpdateVariable(variable.id, variable)
  }

  private handleDeleteVariable = (variable: Variable): void => {
    const {onDeleteVariable} = this.props

    onDeleteVariable(variable.id)
  }
}

const mstp = (state: AppState): StateProps => {
  const variables = extractVariablesList(state)

  return {variables}
}

const mdtp: DispatchProps = {
  onUpdateVariable: updateVariable,
  onDeleteVariable: deleteVariable,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<{}>(VariablesTab))
