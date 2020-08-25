// Libraries
import React from 'react'

// Components
import {Page, PageHeader} from '@influxdata/clockface'
import FlowCreateButton from 'src/notebooks/components/FlowCreateButton'
import NotebookListProvider from 'src/notebooks/context/notebook.list'
import FlowCards from 'src/notebooks/components/FlowCards'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

const FlowsIndex = () => {
  return (
    <NotebookListProvider>
      <Page titleTag={pageTitleSuffixer(['Flows'])} testID="flows-index">
        <PageHeader fullWidth={false}>
          <Page.Title title="Flows" />
        </PageHeader>
        <Page.ControlBar fullWidth={false}>
          <Page.ControlBarLeft></Page.ControlBarLeft>
          <Page.ControlBarRight>
            <FlowCreateButton />
          </Page.ControlBarRight>
        </Page.ControlBar>
        <FlowCards />
      </Page>
    </NotebookListProvider>
  )
}

export default FlowsIndex
