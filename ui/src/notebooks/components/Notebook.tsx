import React, {FC} from 'react'

import {Page} from '@influxdata/clockface'
import {NotebookProvider} from 'src/notebooks/context/notebook'
import Header from 'src/notebooks/components/Header'
import PipeList from 'src/notebooks/components/PipeList'
import AddButtons from 'src/notebooks/components/AddButtons'

// NOTE: uncommon, but using this to scope the project
// within the page and not bleed it's dependancies outside
// of the feature flag
import 'src/notebooks/style.scss'

const NotebookPage: FC = () => {
  return (
    <NotebookProvider>
      <Page titleTag="Notebook">
        <Page.Header fullWidth={true}>
          <Header />
        </Page.Header>
        <Page.ControlBar fullWidth={true}>
          <Page.ControlBarLeft>
            <AddButtons />
          </Page.ControlBarLeft>
        </Page.ControlBar>
        <Page.Contents fullWidth={true} scrollable={true}>
          <PipeList />
        </Page.Contents>
      </Page>
    </NotebookProvider>
  )
}

export default NotebookPage
