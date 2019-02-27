// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Utils
import {getVariablesForOrg} from 'src/variables/selectors'
import {
  getVariables,
  createVariable,
  updateVariable,
  deleteVariable,
} from 'src/variables/actions'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import CreateVariableOverlay from 'src/organizations/components/CreateVariableOverlay'
import {Button, ComponentSize, TechnoSpinner} from '@influxdata/clockface'
import VariableList from 'src/organizations/components/VariableList'
import {Input, OverlayTechnology, EmptyState} from 'src/clockface'
import FilterList from 'src/shared/components/Filter'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'

// Types
import {ComponentColor, IconFont} from '@influxdata/clockface'
import {OverlayState, RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'
import {Variable, Organization} from '@influxdata/influx'

interface StateProps {
  variables: Variable[]
  variablesStatus: RemoteDataState
}

interface DispatchProps {
  onGetVariables: typeof getVariables
  onCreateVariable: typeof createVariable
  onUpdateVariable: typeof updateVariable
  onDeleteVariable: typeof deleteVariable
}

interface OwnProps {
  org: Organization
}

type Props = StateProps & DispatchProps & OwnProps

interface State {
  searchTerm: string
  createOverlayState: OverlayState
  importOverlayState: OverlayState
}

class Variables extends PureComponent<Props, State> {
  public state: State = {
    searchTerm: '',
    createOverlayState: OverlayState.Closed,
    importOverlayState: OverlayState.Closed,
  }

  public componentDidMount() {
    const {variablesStatus, onGetVariables} = this.props

    if (variablesStatus === RemoteDataState.NotStarted) {
      onGetVariables()
    }
  }

  public render() {
    const {variables, variablesStatus, org} = this.props
    const {searchTerm, createOverlayState} = this.state

    if (variablesStatus !== RemoteDataState.Done) {
      return <TechnoSpinner />
    }

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
          list={variables}
        >
          {variables => (
            <VariableList
              variables={variables}
              emptyState={this.emptyState}
              onDeleteVariable={this.handleDeleteVariable}
              onUpdateVariable={this.handleUpdateVariable}
            />
          )}
        </FilterList>
        <OverlayTechnology visible={createOverlayState === OverlayState.Open}>
          <CreateVariableOverlay
            onCreateVariable={this.handleCreateVariable}
            onCloseModal={this.handleCloseCreateOverlay}
            orgID={org.id}
          />
        </OverlayTechnology>
      </>
    )
  }

  private get emptyState(): JSX.Element {
    const {org} = this.props
    const {searchTerm} = this.state

    if (!searchTerm) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text
            text={`${
              org.name
            } does not own any Variables , why not create one?`}
            highlightWords={['Variables']}
          />
          <Button
            size={ComponentSize.Medium}
            text="Create Variable"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            onClick={this.handleOpenCreateOverlay}
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

  private handleCreateVariable = (variable: Variable): void => {
    // TODO(chnn): Remove this handler in favor of connecting child components
    // directly to Redux, and the same for `handleUpdateVariable` and
    // `handleDeleteVariable`
    const {onCreateVariable} = this.props

    onCreateVariable(variable)
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

const mstp = (state: AppState, ownProps: OwnProps): StateProps => {
  const variables = getVariablesForOrg(state, ownProps.org.id)
  const {status: variablesStatus} = state.variables

  return {variables, variablesStatus}
}

const mdtp = {
  onGetVariables: getVariables,
  onCreateVariable: createVariable,
  onUpdateVariable: updateVariable,
  onDeleteVariable: deleteVariable,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(Variables)
