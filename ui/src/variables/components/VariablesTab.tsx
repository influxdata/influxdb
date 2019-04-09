// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Utils
import {
  createVariable,
  updateVariable,
  deleteVariable,
} from 'src/variables/actions'
import {extractVariablesList} from 'src/variables/selectors'

// Components
import {Input, EmptyState} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import CreateVariableOverlay from 'src/variables/components/CreateVariableOverlay'
import VariableList from 'src/variables/components/VariableList'
import FilterList from 'src/shared/components/Filter'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import GetLabels from 'src/configuration/components/GetLabels'

// Types
import {OverlayState} from 'src/types'
import {AppState} from 'src/types'
import {IVariable as Variable} from '@influxdata/influx'
import {IconFont, ComponentSize} from '@influxdata/clockface'

interface StateProps {
  variables: Variable[]
}

interface DispatchProps {
  onCreateVariable: typeof createVariable
  onUpdateVariable: typeof updateVariable
  onDeleteVariable: typeof deleteVariable
}

type Props = StateProps & DispatchProps & WithRouterProps

interface State {
  searchTerm: string
  createOverlayState: OverlayState
  importOverlayState: OverlayState
}

class VariablesTab extends PureComponent<Props, State> {
  public state: State = {
    searchTerm: '',
    createOverlayState: OverlayState.Closed,
    importOverlayState: OverlayState.Closed,
  }

  public render() {
    const {variables, onCreateVariable} = this.props
    const {searchTerm, createOverlayState} = this.state

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
        <GetLabels>
          <FilterList<Variable>
            searchTerm={searchTerm}
            searchKeys={['name', 'labels[].name']}
            list={variables}
            sortByKey="name"
          >
            {variables => (
              <VariableList
                variables={variables}
                emptyState={this.emptyState}
                onDeleteVariable={this.handleDeleteVariable}
                onUpdateVariable={this.handleUpdateVariable}
                onFilterChange={this.handleFilterUpdate}
              />
            )}
          </FilterList>
        </GetLabels>
        <Overlay visible={createOverlayState === OverlayState.Open}>
          <CreateVariableOverlay
            onCreateVariable={onCreateVariable}
            onHideOverlay={this.handleCloseCreateOverlay}
          />
        </Overlay>
      </>
    )
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
    this.setState({createOverlayState: OverlayState.Open})
  }

  private handleCloseCreateOverlay = (): void => {
    this.setState({createOverlayState: OverlayState.Closed})
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
  onCreateVariable: createVariable,
  onUpdateVariable: updateVariable,
  onDeleteVariable: deleteVariable,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<{}>(VariablesTab))
