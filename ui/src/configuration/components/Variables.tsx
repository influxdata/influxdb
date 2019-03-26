// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Utils
import {
  getVariables,
  createVariable,
  updateVariable,
  deleteVariable,
} from 'src/variables/actions'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import CreateVariableOverlay from 'src/configuration/components/CreateVariableOverlay'
import {ComponentSize, RemoteDataState} from '@influxdata/clockface'
import VariableList from 'src/organizations/components/VariableList'
import {Input, EmptyState} from 'src/clockface'
import FilterList from 'src/shared/components/Filter'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'

// Types
import {IconFont} from '@influxdata/clockface'
import {OverlayState} from 'src/types'
import {AppState} from 'src/types'
import {Variable, Organization} from '@influxdata/influx'
import {VariablesState} from 'src/variables/reducers'

interface StateProps {
  variables: VariablesState
  orgs: Organization[]
}

interface DispatchProps {
  onCreateVariable: typeof createVariable
  onUpdateVariable: typeof updateVariable
  onDeleteVariable: typeof deleteVariable
}

type Props = StateProps & DispatchProps

interface State {
  searchTerm: string
  createOverlayState: OverlayState
  importOverlayState: OverlayState
  variables: Variable[]
}

class Variables extends PureComponent<Props, State> {
  public state: State = {
    searchTerm: '',
    createOverlayState: OverlayState.Closed,
    importOverlayState: OverlayState.Closed,
    variables: this.variableList(this.props.variables),
  }

  public render() {
    const {orgs, variables} = this.props
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
        <FilterList<Variable>
          searchTerm={searchTerm}
          searchKeys={['name']}
          list={this.variableList(variables)}
          sortByKey="name"
        >
          {v => (
            <VariableList
              variables={v}
              emptyState={this.emptyState}
              onDeleteVariable={this.handleDeleteVariable}
              onUpdateVariable={this.handleUpdateVariable}
            />
          )}
        </FilterList>
        <CreateVariableOverlay
          onCreateVariable={this.handleCreateVariable}
          onHideOverlay={this.handleCloseCreateOverlay}
          orgs={orgs}
          visible={createOverlayState === OverlayState.Open}
        />
      </>
    )
  }

  private get emptyState(): JSX.Element {
    const {orgs} = this.props
    const {searchTerm} = this.state

    if (!searchTerm) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text
            text={`${
              orgs[0].name
            } does not own any Variables , why not create one?`}
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

  private variableList(variables: VariablesState): Variable[] {
    return Object.values(variables.variables)
      .filter(d => d.status === RemoteDataState.Done)
      .map(d => d.variable)
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target
    this.setState({searchTerm: value})
  }

  private handleFilterBlur() {}

  private handleOpenImportOverlay = (): void => {}

  private handleOpenCreateOverlay = (): void => {
    this.setState({createOverlayState: OverlayState.Open})
  }

  private handleCloseCreateOverlay = (): void => {
    this.setState({createOverlayState: OverlayState.Closed})
  }

  private handleCreateVariable = async (variable: Variable): Promise<void> => {
    // TODO(chnn): Remove this handler in favor of connecting child components
    // directly to Redux, and the same for `handleUpdateVariable` and
    // `handleDeleteVariable`
    const {onCreateVariable} = this.props
    try {
      await onCreateVariable(variable)
      this.handleCloseCreateOverlay()
    } catch (e) {}
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

const mstp = ({variables, orgs}: AppState): StateProps => {
  return {
    variables: variables,
    orgs,
  }
}

const mdtp = {
  onGetVariables: getVariables,
  onCreateVariable: createVariable,
  onUpdateVariable: updateVariable,
  onDeleteVariable: deleteVariable,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(Variables)
