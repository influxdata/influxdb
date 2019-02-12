// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import CreateVariableOverlay from 'src/organizations/components/CreateVariableOverlay'
import {Button, ComponentSize} from '@influxdata/clockface'
import VariablesList from 'src/organizations/components/VariablesList'
import {
  Input,
  ComponentColor,
  IconFont,
  OverlayTechnology,
  EmptyState,
} from 'src/clockface'
import FilterList from 'src/shared/components/Filter'

// Types
import {OverlayState} from 'src/types'
import {client} from 'src/utils/api'
import {Macro} from '@influxdata/influx'

interface Props {
  onChange: () => void
  variables: Macro[]
  orgName: string
  orgID: string
}

interface State {
  searchTerm: string
  overlayState: OverlayState
}

export default class Variables extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = {
      searchTerm: '',
      overlayState: OverlayState.Closed,
    }
  }

  public render() {
    const {variables, orgID} = this.props
    const {searchTerm, overlayState} = this.state

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
          <Button
            text="Create Variable"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            onClick={this.handleOpenModal}
          />
        </TabbedPageHeader>
        <FilterList<Macro>
          searchTerm={searchTerm}
          searchKeys={['name', 'ruleString']}
          list={variables}
        >
          {variables => (
            <VariablesList variables={variables} emptyState={this.emptyState} />
          )}
        </FilterList>
        <OverlayTechnology visible={overlayState === OverlayState.Open}>
          <CreateVariableOverlay
            onCreateVariable={this.handleCreateVariable}
            onCloseModal={this.handleCloseModal}
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

  private handleOpenModal = (): void => {
    this.setState({overlayState: OverlayState.Open})
  }

  private handleCloseModal = (): void => {
    this.setState({overlayState: OverlayState.Closed})
  }

  private handleCreateVariable = async (variable: Macro) => {
    await client.variables.create(variable)
    this.props.onChange()
    this.handleCloseModal()
  }

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
            onClick={this.handleOpenModal}
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
}
