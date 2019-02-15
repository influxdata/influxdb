// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import CreateVariableOverlay from 'src/organizations/components/CreateVariableOverlay'
import {Button, ComponentSize} from '@influxdata/clockface'
import VariableList from 'src/organizations/components/VariableList'
import {Input, OverlayTechnology, EmptyState} from 'src/clockface'
import FilterList from 'src/shared/components/Filter'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'

// Actions
import * as NotificationsActions from 'src/types/actions/notifications'

// Types
import {ComponentColor, IconFont} from '@influxdata/clockface'
import {OverlayState} from 'src/types'
import {client} from 'src/utils/api'
import {Variable} from '@influxdata/influx'
import {
  addVariableFailed,
  deleteVariableFailed,
  addVariableSuccess,
  deleteVariableSuccess,
} from 'src/shared/copy/notifications'

interface Props {
  onChange: () => void
  variables: Variable[]
  orgName: string
  orgID: string
  notify: NotificationsActions.PublishNotificationActionCreator
}

interface State {
  searchTerm: string
  createOverlayState: OverlayState
  importOverlayState: OverlayState
}

export default class Variables extends PureComponent<Props, State> {
  private get emptyState(): JSX.Element {
    const {orgName} = this.props
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`${orgName} does not own any Variables , why not create one?`}
            highlightWords={['Buckets']}
          />
          <Button
            text="Create Variable"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            onClick={this.handleOpenCreateOverlay}
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Variables match your query" />
      </EmptyState>
    )
  }
  constructor(props: Props) {
    super(props)
    this.state = {
      searchTerm: '',
      createOverlayState: OverlayState.Closed,
      importOverlayState: OverlayState.Closed,
    }
  }

  public render() {
    const {variables, orgID} = this.props
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
          list={variables}
        >
          {variables => (
            <VariableList
              variables={variables}
              emptyState={this.emptyState}
              onDeleteVariable={this.handleDeleteVariable}
            />
          )}
        </FilterList>
        <OverlayTechnology visible={createOverlayState === OverlayState.Open}>
          <CreateVariableOverlay
            onCreateVariable={this.handleCreateVariable}
            onCloseModal={this.handleCloseCreateOverlay}
            orgID={orgID}
          />
        </OverlayTechnology>
      </>
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

  private handleCreateVariable = async (variable: Variable): Promise<void> => {
    const {notify, onChange} = this.props

    try {
      await client.variables.create(variable)
      notify(addVariableSuccess(variable.name))
    } catch (error) {
      notify(addVariableFailed())
    }

    onChange()
    this.handleCloseCreateOverlay()
  }

  private handleDeleteVariable = async (variable: Variable): Promise<void> => {
    const {notify, onChange} = this.props

    try {
      await client.variables.delete(variable.id)
      notify(deleteVariableSuccess(variable.name))
    } catch (error) {
      notify(deleteVariableFailed())
    }

    onChange()
  }
}
