// Libraries
import React, {PureComponent} from 'react'

// Components
import DataExplorer from 'src/dataExplorer/components/DataExplorer'
import TimeMachineTabs from 'src/shared/components/TimeMachineTabs'
import {Page} from 'src/pageLayout'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'

interface State {
  activeTab: TimeMachineTab
}

class DataExplorerPage extends PureComponent<null, State> {
  constructor(props) {
    super(props)

    this.state = {
      activeTab: TimeMachineTab.Queries,
    }
  }

  public render() {
    const {activeTab} = this.state

    return (
      <Page>
        <Page.Header>
          <Page.Header.Left>
            <Page.Title title="Data Explorer" />
          </Page.Header.Left>
          <Page.Header.Center>
            <TimeMachineTabs
              activeTab={activeTab}
              onSetActiveTab={this.handleSetActiveTab}
            />
          </Page.Header.Center>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={true} scrollable={false}>
          <DataExplorer activeTab={activeTab} />
        </Page.Contents>
      </Page>
    )
  }

  private handleSetActiveTab = (activeTab: TimeMachineTab): void => {
    this.setState({activeTab})
  }
}

export default DataExplorerPage
