// Libraries
import React, {SFC} from 'react'

// Components
import DataExplorer from 'src/dataExplorer/components/DataExplorer'
import {Page} from 'src/pageLayout'
import SaveAsButton from 'src/dataExplorer/components/SaveAsButton'
import VisOptionsButton from 'src/timeMachine/components/VisOptionsButton'
import ViewTypeDropdown from 'src/timeMachine/components/view_options/ViewTypeDropdown'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'

const DataExplorerPage: SFC = ({children}) => {
  return (
    <Page titleTag="Data Explorer">
      {children}
      <Page.Header fullWidth={true}>
        <Page.Header.Left>
          <PageTitleWithOrg title="Data Explorer" />
        </Page.Header.Left>
        <Page.Header.Right>
          <ViewTypeDropdown />
          <VisOptionsButton />
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
