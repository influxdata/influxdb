import React, {FC} from 'react'

import {Page} from '@influxdata/clockface'
import {NotebookProvider} from 'src/notebooks/context/notebook'
import {ScrollProvider} from 'src/notebooks/context/scroll'
import Header from 'src/notebooks/components/header'
import PipeList from 'src/notebooks/components/PipeList'
import MiniMap from 'src/notebooks/components/minimap/MiniMap'

// NOTE: uncommon, but using this to scope the project
// within the page and not bleed it's dependancies outside
// of the feature flag
import 'src/notebooks/style.scss'

const NotebookPage: FC = () => {
  return (
    <NotebookProvider>
      <ScrollProvider>
        <Page titleTag="Notebook">
          <Header />
          <Page.Contents
            fullWidth={true}
            scrollable={false}
            className="notebook-page"
          >
            <div className="notebook">
              <MiniMap />
              <PipeList />
            </div>
          </Page.Contents>
        </Page>
      </ScrollProvider>
    </NotebookProvider>
  )
}

export default NotebookPage
