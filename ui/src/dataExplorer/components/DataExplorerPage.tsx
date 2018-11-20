// Libraries
import React, {SFC} from 'react'

// Components
import DataExplorer from 'src/dataExplorer/components/DataExplorer'
import TimeMachineTabs from 'src/shared/components/TimeMachineTabs'
import {Page} from 'src/pageLayout'

const DataExplorerPage: SFC<{}> = () => {
  return (
    <Page>
      <Page.Header fullWidth={true}>
        <Page.Header.Left>
          <Page.Title title="Data Explorer" />
        </Page.Header.Left>
        <Page.Header.Center>
          <TimeMachineTabs />
        </Page.Header.Center>
        <Page.Header.Right />
      </Page.Header>
      <Page.Contents fullWidth={true} scrollable={false}>
        <DataExplorer />
      </Page.Contents>
    </Page>
  )
}

export default DataExplorerPage
