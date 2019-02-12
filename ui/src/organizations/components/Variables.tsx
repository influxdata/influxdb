// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import CreateVariableOverlay from 'src/organizations/components/CreateVariableOverlay'
import {Button} from '@influxdata/clockface'
import {Input, ComponentColor, IconFont, OverlayTechnology} from 'src/clockface'

// Types
import {OverlayState} from 'src/types'

interface Props {}

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
        <OverlayTechnology visible={overlayState === OverlayState.Open}>
          <CreateVariableOverlay
            onCreateVariable={this.handleCreateVariable}
            onCloseModal={this.handleCloseModal}
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

  private handleCreateVariable() {}
}
