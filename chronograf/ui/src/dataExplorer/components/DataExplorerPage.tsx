import React, {PureComponent} from 'react'

import DataExplorer from 'src/dataExplorer/components/DataExplorer'
import {Page} from 'src/pageLayout'

class DataExplorerPage extends PureComponent {
  public render() {
    return (
      <Page>
        <Page.Header>
          <Page.Header.Left>
            <Page.Title title="Data Explorer" />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={true} scrollable={false}>
          <DataExplorer />
        </Page.Contents>
      </Page>
    )
  }
}

export default DataExplorerPage
