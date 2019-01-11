// Libraries
import React, {SFC} from 'react'

// Components
import DataExplorer from 'src/dataExplorer/components/DataExplorer'
import {Page} from 'src/pageLayout'
import SaveAsButton from 'src/dataExplorer/components/SaveAsButton'

const DataExplorerPage: SFC<{}> = () => {
  return (
    <Page titleTag="Data Explorer">
      <Page.Header fullWidth={true}>
        <Page.Header.Left>
          <Page.Title title="Data Explorer" />
        </Page.Header.Left>
        <Page.Header.Right>
          <SaveAsButton />
        </Page.Header.Right>
      </Page.Header>
      <Page.Contents fullWidth={true} scrollable={false}>
        <DataExplorer />
      </Page.Contents>
    </Page>
  )
}

export default DataExplorerPage
