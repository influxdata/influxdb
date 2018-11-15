// Libraries
import React, {PureComponent} from 'react'

// Components
import {Page} from 'src/pageLayout'
import {
  Button,
  IconFont,
  ComponentColor,
  OverlayTechnology,
} from 'src/clockface'
import SourcesList from 'src/sources/components/SourcesList'
import CreateSourceOverlay from 'src/sources/components/CreateSourceOverlay'

interface Props {}

interface State {
  isAddingSource: boolean
}

class SourcesPage extends PureComponent<Props, State> {
  public state: State = {isAddingSource: false}

  public render() {
    const {isAddingSource} = this.state

    return (
      <Page>
        <Page.Header>
          <Page.Header.Left>
            <Page.Title title="Manage Sources" />
          </Page.Header.Left>
          <Page.Header.Right>
            <Button
              text="Create Source"
              icon={IconFont.Plus}
              color={ComponentColor.Primary}
              onClick={this.handleShowOverlay}
            />
          </Page.Header.Right>
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={true}>
          <SourcesList />
          <OverlayTechnology visible={isAddingSource}>
            <CreateSourceOverlay onHide={this.handleHideOverlay} />
          </OverlayTechnology>
        </Page.Contents>
      </Page>
    )
  }

  private handleShowOverlay = () => {
    this.setState({isAddingSource: true})
  }

  private handleHideOverlay = () => {
    this.setState({isAddingSource: false})
  }
}

export default SourcesPage
