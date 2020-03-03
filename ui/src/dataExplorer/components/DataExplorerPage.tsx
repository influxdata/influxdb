// Libraries
import React, {SFC} from 'react'

// Components
import DataExplorer from 'src/dataExplorer/components/DataExplorer'
import {Page} from '@influxdata/clockface'
import SaveAsButton from 'src/dataExplorer/components/SaveAsButton'
import VisOptionsButton from 'src/timeMachine/components/VisOptionsButton'
import ViewTypeDropdown from 'src/timeMachine/components/view_options/ViewTypeDropdown'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'
import GetResources from 'src/resources/components/GetResources'
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
        <Page.ControlBar fullWidth={true}>
          <Page.ControlBarLeft>
            <PageTitleWithOrg title="Data Explorer" />
          </Page.ControlBarLeft>
          <Page.ControlBarRight>
            <DeleteDataButton />
            <TimeZoneDropdown />
            <ViewTypeDropdown />
            <VisOptionsButton />
            <SaveAsButton />
          </Page.ControlBarRight>
        </Page.ControlBar>
        <Page.Contents fullWidth={true} scrollable={false}>
          <DataExplorer />
        </Page.Contents>
      </GetResources>
    </Page>
  )
}

export default DataExplorerPage
