// Libraries
import React, {SFC} from 'react'

// Components
import DataExplorer from 'src/dataExplorer/components/DataExplorer'
import {Page} from '@influxdata/clockface'
import SaveAsButton from 'src/dataExplorer/components/SaveAsButton'
import VisOptionsButton from 'src/timeMachine/components/VisOptionsButton'
import ViewTypeDropdown from 'src/timeMachine/components/view_options/ViewTypeDropdown'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'
import GetResources from 'src/shared/components/GetResources'
import TimeZoneDropdown from 'src/shared/components/TimeZoneDropdown'
import DeleteDataButton from 'src/dataExplorer/components/DeleteDataButton'

// Types
import {ResourceType} from 'src/types'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

const DataExplorerPage: SFC = ({children}) => {
  return (
    <Page titleTag={pageTitleSuffixer(['Data Explorer'])}>
      {children}
      <GetResources resources={[ResourceType.Variables]}>
        <Page.Header fullWidth={true}>
          <Page.HeaderLeft>
            <PageTitleWithOrg title="Data Explorer" />
          </Page.HeaderLeft>
          <Page.HeaderRight>
            <DeleteDataButton />
            <TimeZoneDropdown />
            <ViewTypeDropdown />
            <VisOptionsButton />
            <SaveAsButton />
          </Page.HeaderRight>
        </Page.Header>
        <Page.Contents fullWidth={true} scrollable={false}>
          <DataExplorer />
        </Page.Contents>
      </GetResources>
    </Page>
  )
}

export default DataExplorerPage
